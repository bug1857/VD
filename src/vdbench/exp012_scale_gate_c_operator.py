"""Scale-specific Gate-C operator with telemetry and bounded checkpoints.

The accepted detector/attempt/acknowledgement/attestation/finalization pipeline
is reused unchanged. EXP-012 adds a distinct full plan/result namespace, exact
scale completion checks, additive telemetry, and the separately confirmed,
non-authorizing bounded-checkpoint path governed by ADR-016/ADR-019.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

from .canonical_serialization import (
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from .config import Metric
from .exp010_gate_c_operator import (
    OPERAND_FIELDS as EXP010_OPERAND_FIELDS,
    Exp010GateCOperands,
    Exp010GateCOperatorError,
    build_gate_c_plan as build_exp010_gate_c_plan,
    run_gate_c_execute_from_cli,
)
from .exp010_serving_configuration import (
    derive_serving_configuration_identity,
    validate_governed_configuration_identity,
)
from .exp012_scale_contract import (
    Exp012ScaleContract,
    Exp012ScaleProfile,
    build_exp012_scale_contract,
    exp012_scale_contract_payload,
    verify_exp012_scale_contract,
)
from .gate_c_bounded_execution import (
    GateCBoundedExecutionEnvelope,
    GateCBoundedExecutionError,
    GateCWindowExecutionBound,
    build_gate_c_bounded_execution_envelope,
    build_gate_c_canonical_state,
    build_gate_c_checkpoint_result,
    build_gate_c_window_checkpoint_effect,
    gate_c_bounded_execution_envelope_document,
    parse_gate_c_bounded_execution_envelope_document,
    verify_gate_c_bounded_execution_envelope,
)
from .gate_c_window_execution import GateCWindowExecutionError
from .gate_c_checkpoint_store import (
    GateCCheckpointLedgerBinding,
    GateCCheckpointLedgerError,
    SQLiteGateCCheckpointLedger,
)
from .host_window_lineage import HostResponseCommitError, SQLiteHostResponseCommitStore
from .shadow_attempt_store import ShadowAttemptStatus
from .shadow_window import TRACE_COUNT, WINDOW_QUERY_COUNT
from .shadow_search_telemetry import (
    SQLiteShadowSearchTelemetryStore,
    ShadowSearchTelemetryBinding,
    ShadowSearchTelemetryError,
)

__all__ = [
    "EXP012_GATE_C_PLAN_SCHEMA_VERSION",
    "Exp012ScaleGateCOperands",
    "Exp012ScaleGateCOperatorError",
    "build_gate_c_plan",
    "build_gate_c_checkpoint_envelope",
    "load_operands",
    "main",
    "run_gate_c",
    "run_gate_c_checkpoint",
]


EXP012_GATE_C_PLAN_SCHEMA_VERSION = "exp012-scale-gate-c-plan-v1"
_RESULT_SCHEMA_VERSION = "exp012-scale-gate-c-result-v1"
_PLAN_DOMAIN = b"VD::EXP012_SCALE_GATE_C_PLAN::V1\x00"
_RESULT_DOMAIN = b"VD::EXP012_SCALE_GATE_C_RESULT::V1\x00"
_FIELDS = (
    *(name for name in EXP010_OPERAND_FIELDS if name != "exp010_output_dir"),
    "scale_profile",
    "scale_output_dir",
    "gate_a_campaign_root",
)
_TELEMETRY_FILENAME = "exp012_shadow_search_telemetry.sqlite3"
_CHECKPOINT_FILENAME = "exp012_gate_c_checkpoints.sqlite3"
_PLAN_DOMAIN_BYTES = b"VD::EXP012_SCALE_GATE_C_PLAN::V1\x00"


class Exp012ScaleGateCOperatorError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> Exp012ScaleGateCOperatorError:
    return Exp012ScaleGateCOperatorError(code)


@dataclass(frozen=True, slots=True)
class Exp012ScaleGateCOperands:
    contract: Exp012ScaleContract
    base: Exp010GateCOperands
    gate_a_campaign_root: Path

    @property
    def telemetry_path(self) -> Path:
        return self.base.store_root / _TELEMETRY_FILENAME

    @property
    def checkpoint_path(self) -> Path:
        return self.base.store_root / _CHECKPOINT_FILENAME


def _text(values: dict[str, object], name: str) -> str:
    value = values[name]
    if type(value) is not str or not value or value != value.strip():
        raise _error("EXP012_GATE_C_OPERAND_INVALID")
    return value


def _integer(values: dict[str, object], name: str) -> int:
    value = values[name]
    if type(value) is not int:
        raise _error("EXP012_GATE_C_OPERAND_INVALID")
    return value


def _real(values: dict[str, object], name: str) -> float:
    value = values[name]
    if type(value) not in (int, float):
        raise _error("EXP012_GATE_C_OPERAND_INVALID")
    return float(value)


def load_operands(path: str | os.PathLike[str]) -> Exp012ScaleGateCOperands:
    try:
        values = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _error("EXP012_GATE_C_OPERANDS_MALFORMED") from exc
    if type(values) is not dict or set(values) != set(_FIELDS):
        raise _error("EXP012_GATE_C_OPERANDS_INVALID")
    try:
        profile = Exp012ScaleProfile(_text(values, "scale_profile"))
        metric = Metric(_text(values, "metric"))
    except ValueError as exc:
        raise _error("EXP012_GATE_C_OPERAND_INVALID") from exc
    configuration_identity = validate_governed_configuration_identity(
        values["configuration_identity"]
    )
    base = Exp010GateCOperands(
        stream_id=_text(values, "stream_id"),
        metric=metric,
        threshold_stratum=_text(values, "threshold_stratum"),
        threshold_radius=_real(values, "threshold_radius"),
        range_filter=_real(values, "range_filter"),
        limit=_integer(values, "limit"),
        served_ef=_integer(values, "served_ef"),
        dimensions=_integer(values, "dimensions"),
        consistency_level=_text(values, "consistency_level"),
        configuration_identity=configuration_identity,
        flat_binding_id=_text(values, "flat_binding_id"),
        hnsw_binding_id=_text(values, "hnsw_binding_id"),
        source_revision=_text(values, "source_revision"),
        environment_manifest_sha256=_text(values, "environment_manifest_sha256"),
        detector_seed=_integer(values, "detector_seed"),
        milvus_uri=_text(values, "milvus_uri"),
        flat_collection_name=_text(values, "flat_collection_name"),
        hnsw_collection_name=_text(values, "hnsw_collection_name"),
        store_root=Path(_text(values, "store_root")),
        dataset001_dir=Path(_text(values, "dataset001_dir")),
        # The reused runner field is only a filesystem destination; the public
        # EXP-012 operand and emitted plan never call it EXP-010 evidence.
        exp010_output_dir=Path(_text(values, "scale_output_dir")),
        etcd_container=_text(values, "etcd_container"),
        minio_container=_text(values, "minio_container"),
    )
    if len(base.environment_manifest_sha256) != 64 or any(
        item not in "0123456789abcdef" for item in base.environment_manifest_sha256
    ):
        raise _error("EXP012_GATE_C_OPERAND_INVALID")
    if derive_serving_configuration_identity(base.serving_configuration) != configuration_identity:
        raise _error("EXP012_GATE_C_CONFIGURATION_IDENTITY_MISMATCH")
    if base.flat_collection_name == base.hnsw_collection_name:
        raise _error("EXP012_GATE_C_OPERAND_INVALID")
    if base.flat_binding_id == base.hnsw_binding_id:
        raise _error("EXP012_GATE_C_OPERAND_INVALID")
    return Exp012ScaleGateCOperands(
        contract=build_exp012_scale_contract(profile),
        base=base,
        gate_a_campaign_root=Path(_text(values, "gate_a_campaign_root")),
    )


def build_gate_c_plan(operands: Exp012ScaleGateCOperands) -> dict[str, object]:
    if type(operands) is not Exp012ScaleGateCOperands:
        raise _error("EXP012_GATE_C_OPERANDS_INVALID")
    contract = verify_exp012_scale_contract(operands.contract)
    if (
        type(operands.base) is not Exp010GateCOperands
        or not isinstance(operands.gate_a_campaign_root, Path)
        or operands.gate_a_campaign_root == operands.base.store_root.parent
    ):
        raise _error("EXP012_GATE_C_OPERANDS_INVALID")
    base = build_exp010_gate_c_plan(
        operands.base,
        accepted_scale_contract_sha256=contract.contract_sha256,
        authority_campaign_root=operands.gate_a_campaign_root,
    )
    from .exp012_scale_campaign import load_scale_campaign_marker

    load_scale_campaign_marker(
        operands.base.store_root.parent,
        expected_contract=contract,
        expected_gate_a_evidence_sha256=base["gate_a_authority"]["evidence_sha256"],
    )
    observed = base["observed"]
    binding = _telemetry_binding(operands)
    source_count = _verified_source_count(operands, binding)
    if source_count != contract.target_source_records:
        raise _error("EXP012_GATE_C_SOURCE_TARGET_INCOMPLETE")
    if observed["complete_source_windows"] != contract.expected_windows:
        raise _error("EXP012_GATE_C_SOURCE_TARGET_INCOMPLETE")
    if observed["shadow_acknowledged_count"] > contract.target_source_records:
        raise _error("EXP012_GATE_C_PROGRESS_INVALID")
    if observed["next_window_sequence"] > contract.expected_windows:
        raise _error("EXP012_GATE_C_PROGRESS_INVALID")
    plan: dict[str, object] = {
        "schema_version": EXP012_GATE_C_PLAN_SCHEMA_VERSION,
        "experiment_id": "EXP-012-SCALE",
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "canonical_entrypoint": base["canonical_entrypoint"],
        "canonical_composition": base["canonical_composition"],
        "canonical_capture_executor": base["canonical_capture_executor"],
        "stream": base["stream"],
        "source_revision": base["source_revision"],
        "environment_manifest_sha256": base["environment_manifest_sha256"],
        "gate_a_authority": base["gate_a_authority"],
        "detector_seed": base["detector_seed"],
        "serving": base["serving"],
        "milvus": base["milvus"],
        "stores": base["stores"],
        "telemetry_path": str(operands.telemetry_path),
        "dataset001_dir": base["dataset001_dir"],
        "observed": {**observed, "source_count": source_count},
        "projected_physical_work": base["projected_physical_work"],
        "physical_searches_issued_by_preflight": 0,
        "serve_calls_issued_by_gate_c": 0,
    }
    plan["plan_sha256"] = strict_canonical_digest(_PLAN_DOMAIN, plan)
    return plan


def _verified_source_count(
    operands: Exp012ScaleGateCOperands,
    binding: ShadowSearchTelemetryBinding,
) -> int:
    with SQLiteHostResponseCommitStore(
        operands.base.store_root / "v2_source.sqlite3",
        stream_key=binding.stream_key,
        source_revision=operands.base.source_revision,
        environment_manifest_sha256=operands.base.environment_manifest_sha256,
        consistency_level=operands.base.consistency_level,
    ) as source_store:
        source_head = source_store.verified_source_head()
    return source_head.source_count


def _telemetry_binding(
    operands: Exp012ScaleGateCOperands,
) -> ShadowSearchTelemetryBinding:
    from .exp010_gate_c_operator import _stream_key_for

    stream = _stream_key_for(operands.base.runner_configuration(), operands.base)
    return ShadowSearchTelemetryBinding(
        campaign_id=operands.base.store_root.parent.name,
        scale_contract=operands.contract,
        stream_key=stream,
        source_revision=operands.base.source_revision,
        environment_manifest_sha256=operands.base.environment_manifest_sha256,
    )


def _verified_sources_and_head(
    operands: Exp012ScaleGateCOperands,
    binding: ShadowSearchTelemetryBinding,
):
    with SQLiteHostResponseCommitStore(
        operands.base.store_root / "v2_source.sqlite3",
        stream_key=binding.stream_key,
        source_revision=operands.base.source_revision,
        environment_manifest_sha256=operands.base.environment_manifest_sha256,
        consistency_level=operands.base.consistency_level,
    ) as source_store:
        head = source_store.verified_source_head()
        sources = source_store.poll(
            consumer_id="exp012-bounded-envelope-verifier",
            limit=operands.contract.target_source_records + 1,
        )
    if len(sources) != operands.contract.target_source_records:
        raise _error("EXP012_GATE_C_SOURCE_TARGET_INCOMPLETE")
    return sources, head


def _campaign_binding(operands: Exp012ScaleGateCOperands, plan: dict[str, object]):
    from .exp012_scale_campaign import load_scale_campaign_marker

    return load_scale_campaign_marker(
        operands.base.store_root.parent,
        expected_contract=operands.contract,
        expected_gate_a_evidence_sha256=plan["gate_a_authority"]["evidence_sha256"],
    )


def _plan_at_checkpoint_start(
    plan: dict[str, object], bound: GateCWindowExecutionBound
) -> dict[str, object]:
    """Reconstruct the exact pre-checkpoint plan after a completed recovery."""

    normalized = copy.deepcopy(plan)
    observed = normalized["observed"]
    projected = normalized["projected_physical_work"]
    if type(observed) is not dict or type(projected) is not dict:
        raise _error("EXP012_GATE_C_CHECKPOINT_PLAN_INVALID")
    current = observed.get("next_window_sequence")
    if current not in {
        bound.start_window_sequence,
        bound.expected_next_window_sequence,
    }:
        raise _error("EXP012_GATE_C_CHECKPOINT_PROGRESS_INVALID")
    expected_acknowledged = current * WINDOW_QUERY_COUNT
    if observed.get("shadow_acknowledged_count") != expected_acknowledged:
        raise _error("EXP012_GATE_C_CHECKPOINT_PROGRESS_INVALID")
    complete_windows = observed.get("complete_source_windows")
    if type(complete_windows) is not int:
        raise _error("EXP012_GATE_C_CHECKPOINT_PLAN_INVALID")
    observed["shadow_acknowledged_count"] = (
        bound.start_window_sequence * WINDOW_QUERY_COUNT
    )
    observed["next_window_sequence"] = bound.start_window_sequence
    observed["windows_pending"] = complete_windows - bound.start_window_sequence
    projected["flat_searches"] = (
        complete_windows - bound.start_window_sequence
    ) * WINDOW_QUERY_COUNT
    projected["hnsw_sentinel_searches"] = projected["flat_searches"]
    normalized.pop("plan_sha256", None)
    normalized["plan_sha256"] = strict_canonical_digest(
        _PLAN_DOMAIN_BYTES, normalized
    )
    return normalized


def build_gate_c_checkpoint_envelope(
    operands: Exp012ScaleGateCOperands,
    execution_bound: GateCWindowExecutionBound,
) -> GateCBoundedExecutionEnvelope:
    """Build a fresh pre-search envelope from canonical persisted authority."""

    plan = build_gate_c_plan(operands)
    binding = _telemetry_binding(operands)
    sources, source_head = _verified_sources_and_head(operands, binding)
    campaign = _campaign_binding(operands, plan)
    return build_gate_c_bounded_execution_envelope(
        plan=plan,
        campaign_binding=campaign,
        source_head=source_head,
        sources=sources,
        execution_bound=execution_bound,
    )


def _verify_current_checkpoint_envelope(
    operands: Exp012ScaleGateCOperands,
    document: dict[str, object],
) -> tuple[GateCBoundedExecutionEnvelope, dict[str, object], tuple[object, ...]]:
    supplied = parse_gate_c_bounded_execution_envelope_document(document)
    current_plan = build_gate_c_plan(operands)
    normalized_plan = _plan_at_checkpoint_start(
        current_plan, supplied.execution_bound
    )
    binding = _telemetry_binding(operands)
    sources, source_head = _verified_sources_and_head(operands, binding)
    campaign = _campaign_binding(operands, current_plan)
    envelope = verify_gate_c_bounded_execution_envelope(
        document,
        plan=normalized_plan,
        campaign_binding=campaign,
        source_head=source_head,
        sources=sources,
    )
    return envelope, current_plan, sources


def _checkpoint_ledger_binding(
    envelope: GateCBoundedExecutionEnvelope,
) -> GateCCheckpointLedgerBinding:
    return GateCCheckpointLedgerBinding(
        campaign_identity=envelope.campaign_identity,
        campaign_binding_sha256=envelope.campaign_binding_sha256,
        scale_contract_sha256=envelope.scale_contract.contract_sha256,
        source_revision=envelope.source_revision,
    )


def _checkpoint_runner(operands: Exp012ScaleGateCOperands):
    from .exp010_gate_c_operator import MonotonicUtcClock, _RefusingServingExecutor
    from .exp010_live_runner import Exp010LiveRunner

    class _RefusingCapture:
        def capture(self, *_args, **_kwargs):
            raise _error("EXP012_GATE_C_CHECKPOINT_CAPTURE_FORBIDDEN")

    clock = MonotonicUtcClock()
    runner = Exp010LiveRunner(
        configuration=operands.base.runner_configuration(),
        serving_executor=_RefusingServingExecutor(),
        shadow_capture_executor=_RefusingCapture(),
        clock=clock,
        shadow_captured_at_clock=clock,
    )
    return runner


def _verified_checkpoint_transition(
    operands: Exp012ScaleGateCOperands,
    envelope: GateCBoundedExecutionEnvelope,
    sources: tuple[object, ...],
    *,
    current_sequence: int,
) -> tuple[dict[str, object], dict[str, object], tuple[dict[str, object], ...]]:
    """Reconstruct exact pre/post state and reject every future suffix effect."""

    from .exp010_v2_host import SHADOW_CONSUMER_ID
    from .window_finalization import WindowFinalizationPhase

    bound = envelope.execution_bound
    start = bound.start_window_sequence
    if current_sequence not in {start, bound.expected_next_window_sequence}:
        raise _error("EXP012_GATE_C_CHECKPOINT_PROGRESS_INVALID")
    post = current_sequence
    runner = _checkpoint_runner(operands)
    try:
        composition = runner.composition
        finalization_states = composition.finalization_store.states()
        if (
            composition.finalization_store.pending() is not None
            or composition.finalization_store.next_window_sequence() != post
            or len(finalization_states) != post
            or any(
                state.phase is not WindowFinalizationPhase.FINALIZED
                or state.prepared.window_sequence != index
                for index, state in enumerate(finalization_states)
            )
        ):
            raise _error("EXP012_GATE_C_CHECKPOINT_FINALIZATION_SUFFIX_INVALID")

        acknowledgement = composition.response_store.consumer_acknowledgement_state(
            consumer_id=SHADOW_CONSUMER_ID
        )
        expected_event_ids = tuple(
            event_id
            for state in finalization_states
            for event_id in state.prepared.source_event_ids
        )
        if acknowledgement.event_ids != expected_event_ids:
            raise _error("EXP012_GATE_C_CHECKPOINT_ACKNOWLEDGEMENT_INVALID")

        progression = composition.detector_store.load_progression()
        if progression.next_window_sequence != post:
            raise _error("EXP012_GATE_C_CHECKPOINT_DETECTOR_SUFFIX_INVALID")
        detector_total = composition.detector_store.verified_event_head()
        detector_post = composition.detector_store.verified_event_head(
            before_window_sequence=post
        )
        if detector_total != detector_post or detector_post[0] != post:
            raise _error("EXP012_GATE_C_CHECKPOINT_DETECTOR_SUFFIX_INVALID")

        attempts_by_window: dict[int, tuple[object, ...]] = {}
        for window_sequence in range(operands.contract.expected_windows):
            records = composition.shadow_attempt_store.records_for_window(
                window_sequence
            )
            if window_sequence < post:
                if (
                    len(records) != TRACE_COUNT
                    or any(
                        item.status is not ShadowAttemptStatus.COMPLETED
                        for item in records
                    )
                ):
                    raise _error("EXP012_GATE_C_CHECKPOINT_ATTEMPTS_INVALID")
                attempts_by_window[window_sequence] = records
            elif records:
                if any(item.status is ShadowAttemptStatus.ORPHANED for item in records):
                    raise _error("EXP012_GATE_C_CHECKPOINT_ORPHANED_SUFFIX")
                raise _error("EXP012_GATE_C_CHECKPOINT_ATTEMPT_SUFFIX_INVALID")
        attempt_total = composition.shadow_attempt_store.verified_event_head()
        attempt_post = composition.shadow_attempt_store.verified_event_head(
            before_window_sequence=post
        )
        if attempt_total != attempt_post or attempt_post[0] != post * TRACE_COUNT * 2:
            raise _error("EXP012_GATE_C_CHECKPOINT_ATTEMPT_SUFFIX_INVALID")

        attestation_total = composition.attestation_store.verified_record_head()
        attestation_post = composition.attestation_store.verified_record_head(
            before_window_sequence=post
        )
        if attestation_total != attestation_post:
            raise _error("EXP012_GATE_C_CHECKPOINT_ATTESTATION_SUFFIX_INVALID")

        finalization_total = composition.finalization_store.verified_event_head()
        finalization_post = composition.finalization_store.verified_event_head(
            before_window_sequence=post
        )
        if (
            finalization_total != finalization_post
            or finalization_post[0] != post * 5
        ):
            raise _error("EXP012_GATE_C_CHECKPOINT_FINALIZATION_SUFFIX_INVALID")

        telemetry_records: tuple[object, ...]
        if operands.telemetry_path.exists():
            with SQLiteShadowSearchTelemetryStore(
                operands.telemetry_path, binding=_telemetry_binding(operands)
            ) as telemetry:
                if post:
                    telemetry.verify_prefix(
                        tuple(sources[: post * WINDOW_QUERY_COUNT])
                    )
                telemetry_records = telemetry.records()
        else:
            telemetry_records = ()
        expected_telemetry = (
            post
            * WINDOW_QUERY_COUNT
            * operands.contract.searches_per_source
        )
        if len(telemetry_records) != expected_telemetry:
            raise _error("EXP012_GATE_C_CHECKPOINT_TELEMETRY_SUFFIX_INVALID")

        effects: list[dict[str, object]] = []
        for window_sequence, state in enumerate(finalization_states):
            persisted = composition.detector_store.load_persisted_window(
                window_sequence
            )
            records = attempts_by_window[window_sequence]
            if persisted is None:
                raise _error("EXP012_GATE_C_CHECKPOINT_DETECTOR_EFFECT_INVALID")
            detector_head = persisted.result.detector_head
            detector_head_sha256 = (
                None
                if detector_head is None
                else detector_head.detector_head_sha256
            )
            if (
                persisted.event_sha256 != state.detector_event_sha256
                or detector_head_sha256 != state.detector_head_sha256
                or persisted.result.status is not state.prepared.expected_detector_status
                or persisted.source_window_sha256
                != state.prepared.source_window_sha256
                or persisted.shadow_window_sha256
                != state.prepared.shadow_window_sha256
                or tuple(item.identity.attempt_sha256 for item in records)
                != state.prepared.attempt_sha256s
            ):
                raise _error("EXP012_GATE_C_CHECKPOINT_DETECTOR_EFFECT_INVALID")
            if detector_head is None:
                if (
                    state.attestation_record_sha256 is not None
                    or state.attestation_sha256 is not None
                ):
                    raise _error("EXP012_GATE_C_CHECKPOINT_ATTESTATION_EFFECT_INVALID")
                disposition = "NOT_REQUIRED"
                attestation_record_sha256 = None
                attestation_sha256 = None
            else:
                attested = composition.attestation_store.load_for_detector_head(
                    detector_head
                )
                if (
                    attested is None
                    or attested.record_sha256 != state.attestation_record_sha256
                    or attested.attestation.attestation_sha256
                    != state.attestation_sha256
                ):
                    raise _error("EXP012_GATE_C_CHECKPOINT_ATTESTATION_EFFECT_INVALID")
                disposition = "COMMITTED"
                attestation_record_sha256 = attested.record_sha256
                attestation_sha256 = attested.attestation.attestation_sha256
            attestation_record_head_sha256 = (
                composition.attestation_store.verified_record_head(
                    before_window_sequence=window_sequence + 1
                )[1]
            )
            acknowledged_prefix = (
                composition.response_store.consumer_acknowledgement_prefix_state(
                    consumer_id=SHADOW_CONSUMER_ID,
                    acknowledged_count=(window_sequence + 1) * WINDOW_QUERY_COUNT,
                )
            )
            expected_prefix_ids = tuple(
                event_id
                for item in finalization_states[: window_sequence + 1]
                for event_id in item.prepared.source_event_ids
            )
            if (
                acknowledged_prefix.event_ids != expected_prefix_ids
                or acknowledged_prefix.head_sha256
                != state.acknowledgement_head_sha256
            ):
                raise _error("EXP012_GATE_C_CHECKPOINT_ACKNOWLEDGEMENT_INVALID")
            attempt_window_head = composition.shadow_attempt_store.verified_event_head(
                before_window_sequence=window_sequence + 1
            )[1]
            finalization_window_head = composition.finalization_store.verified_event_head(
                before_window_sequence=window_sequence + 1
            )[1]
            telemetry_end = (
                (window_sequence + 1)
                * WINDOW_QUERY_COUNT
                * operands.contract.searches_per_source
            )
            telemetry_window_head = telemetry_records[telemetry_end - 1].record_sha256
            if (
                attempt_window_head is None
                or finalization_window_head is None
                or acknowledged_prefix.head_sha256 is None
            ):
                raise _error("EXP012_GATE_C_CHECKPOINT_EFFECT_INVALID")
            effects.append(
                build_gate_c_window_checkpoint_effect(
                    window_sequence=window_sequence,
                    source_window_sha256=state.prepared.source_window_sha256,
                    attempt_sha256s=state.prepared.attempt_sha256s,
                    attempt_event_head_sha256=attempt_window_head,
                    detector_event_sha256=persisted.event_sha256,
                    detector_status=persisted.result.status.value,
                    detector_head_sha256=detector_head_sha256,
                    attestation_disposition=disposition,
                    attestation_record_sha256=attestation_record_sha256,
                    attestation_record_head_sha256=(
                        attestation_record_head_sha256
                    ),
                    attestation_sha256=attestation_sha256,
                    prepared_sha256=state.prepared.prepared_sha256,
                    acknowledgement_head_sha256=acknowledged_prefix.head_sha256,
                    finalization_event_head_sha256=finalization_window_head,
                    telemetry_record_count=(
                        WINDOW_QUERY_COUNT
                        * operands.contract.searches_per_source
                    ),
                    telemetry_record_head_sha256=telemetry_window_head,
                )
            )

        def state_at(sequence: int) -> dict[str, object]:
            ack = composition.response_store.consumer_acknowledgement_prefix_state(
                consumer_id=SHADOW_CONSUMER_ID,
                acknowledged_count=sequence * WINDOW_QUERY_COUNT,
            )
            attempt_head = composition.shadow_attempt_store.verified_event_head(
                before_window_sequence=sequence
            )
            detector_head = composition.detector_store.verified_event_head(
                before_window_sequence=sequence
            )
            attestation_head = composition.attestation_store.verified_record_head(
                before_window_sequence=sequence
            )
            finalization_head = composition.finalization_store.verified_event_head(
                before_window_sequence=sequence
            )
            telemetry_count = (
                sequence
                * WINDOW_QUERY_COUNT
                * operands.contract.searches_per_source
            )
            telemetry_head = (
                None
                if telemetry_count == 0
                else telemetry_records[telemetry_count - 1].record_sha256
            )
            return build_gate_c_canonical_state(
                next_window_sequence=sequence,
                acknowledgement_count=len(ack.event_ids),
                acknowledgement_head_sha256=ack.head_sha256,
                attempt_count=sequence * TRACE_COUNT,
                attempt_event_count=attempt_head[0],
                attempt_event_head_sha256=attempt_head[1],
                detector_event_count=detector_head[0],
                detector_event_head_sha256=detector_head[1],
                attestation_record_count=attestation_head[0],
                attestation_record_head_sha256=attestation_head[1],
                finalization_window_count=sequence,
                finalization_event_count=finalization_head[0],
                finalization_event_head_sha256=finalization_head[1],
                telemetry_record_count=telemetry_count,
                telemetry_record_head_sha256=telemetry_head,
            )

        return state_at(start), state_at(post), tuple(effects[start:post])
    finally:
        runner.composition.close()


def _require_safe_checkpoint_start(
    operands: Exp012ScaleGateCOperands,
    envelope: GateCBoundedExecutionEnvelope,
    sources: tuple[object, ...],
) -> dict[str, object]:
    pre, post, effects = _verified_checkpoint_transition(
        operands,
        envelope,
        sources,
        current_sequence=envelope.execution_bound.start_window_sequence,
    )
    if pre != post or effects:
        raise _error("EXP012_GATE_C_CHECKPOINT_START_STATE_INVALID")
    return pre


def run_gate_c_checkpoint(
    operands: Exp012ScaleGateCOperands,
    envelope_document: dict[str, object],
) -> dict[str, object]:
    """Execute or reconstruct exactly one governed bounded checkpoint."""

    envelope, current_plan, sources = _verify_current_checkpoint_envelope(
        operands, envelope_document
    )
    bound = envelope.execution_bound
    current_next = current_plan["observed"]["next_window_sequence"]
    ledger_binding = _checkpoint_ledger_binding(envelope)
    with SQLiteGateCCheckpointLedger(
        operands.checkpoint_path, binding=ledger_binding
    ) as ledger:
        state = ledger.state(envelope)
        if state is not None and state.completed_event_sha256 is not None:
            raise _error("EXP012_GATE_C_CHECKPOINT_ALREADY_COMPLETED")
        if state is None:
            if current_next != bound.start_window_sequence:
                raise _error("EXP012_GATE_C_CHECKPOINT_START_MISMATCH")
            _require_safe_checkpoint_start(operands, envelope, sources)
            state = ledger.start(envelope, recorded_at_utc=_checkpoint_clock())
        if current_next == bound.start_window_sequence:
            _require_safe_checkpoint_start(operands, envelope, sources)
            with SQLiteShadowSearchTelemetryStore(
                operands.telemetry_path, binding=_telemetry_binding(operands)
            ) as telemetry:
                results = run_gate_c_execute_from_cli(
                    operands.base,
                    search_telemetry_store=telemetry,
                    accepted_scale_contract_sha256=operands.contract.contract_sha256,
                    execution_bound=bound,
                )
            if tuple(item.window_sequence for item in results) != bound.allowed_window_sequences:
                raise _error("EXP012_GATE_C_CHECKPOINT_RESULT_MISMATCH")
        elif current_next != bound.expected_next_window_sequence:
            raise _error("EXP012_GATE_C_CHECKPOINT_PROGRESS_INVALID")
        pre_state, post_state, checkpoint_effects = _verified_checkpoint_transition(
            operands,
            envelope,
            sources,
            current_sequence=bound.expected_next_window_sequence,
        )
        result = build_gate_c_checkpoint_result(
            envelope=envelope,
            pre_state=pre_state,
            post_state=post_state,
            processed_window_sequences=bound.allowed_window_sequences,
            checkpoint_effects=checkpoint_effects,
        )
        ledger.complete(
            envelope,
            checkpoint_result=result,
            recorded_at_utc=_checkpoint_clock(),
        )
        return result


def _checkpoint_clock() -> str:
    from .exp010_gate_c_operator import MonotonicUtcClock

    return MonotonicUtcClock()()




def run_gate_c(operands: Exp012ScaleGateCOperands) -> dict[str, object]:
    plan = build_gate_c_plan(operands)
    return _run_gate_c_after_plan(operands, plan)


def _run_gate_c_after_plan(
    operands: Exp012ScaleGateCOperands, plan: dict[str, object]
) -> dict[str, object]:
    if plan.get("scale_contract_sha256") != operands.contract.contract_sha256:
        raise _error("EXP012_GATE_C_PLAN_MISMATCH")
    binding = _telemetry_binding(operands)
    with SQLiteShadowSearchTelemetryStore(
        operands.telemetry_path, binding=binding
    ) as telemetry:
        results = run_gate_c_execute_from_cli(
            operands.base,
            search_telemetry_store=telemetry,
            accepted_scale_contract_sha256=operands.contract.contract_sha256,
        )
        with SQLiteHostResponseCommitStore(
            operands.base.store_root / "v2_source.sqlite3",
            stream_key=binding.stream_key,
            source_revision=operands.base.source_revision,
            environment_manifest_sha256=operands.base.environment_manifest_sha256,
            consistency_level=operands.base.consistency_level,
        ) as source_store:
            sources = source_store.poll(
                consumer_id="exp012-scale-telemetry-verifier",
                limit=operands.contract.target_source_records + 1,
            )
        telemetry_summary = telemetry.verify_completion(sources)
    post = build_exp010_gate_c_plan(
        operands.base,
        accepted_scale_contract_sha256=operands.contract.contract_sha256,
        authority_campaign_root=operands.gate_a_campaign_root,
    )
    if post["observed"]["next_window_sequence"] != operands.contract.expected_windows:
        raise _error("EXP012_GATE_C_FINALIZATION_INCOMPLETE")
    if not telemetry_summary.complete:
        raise _error("EXP012_GATE_C_TELEMETRY_INCOMPLETE")
    result: dict[str, object] = {
        "schema_version": _RESULT_SCHEMA_VERSION,
        "experiment_id": "EXP-012-SCALE",
        "scale_contract_sha256": operands.contract.contract_sha256,
        "plan_sha256": plan["plan_sha256"],
        "windows_processed_this_invocation": len(results),
        "finalized_windows": operands.contract.expected_windows,
        "telemetry_record_count": telemetry_summary.record_count,
        "telemetry_head_sha256": telemetry_summary.head_sha256,
        "physical_searches": operands.contract.expected_physical_searches,
    }
    result["result_sha256"] = strict_canonical_digest(_RESULT_DOMAIN, result)
    return result


def _write_checkpoint_envelope(
    path: Path, envelope: GateCBoundedExecutionEnvelope
) -> None:
    raw = strict_canonical_json_bytes(
        gate_c_bounded_execution_envelope_document(envelope)
    )
    if path.exists():
        try:
            info = os.lstat(path)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_UNSAFE")
            if path.read_bytes() == raw:
                return
        except OSError as exc:
            raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_WRITE_FAILED") from exc
        raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_EXISTS")
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_WRITE_FAILED") from exc


def _load_checkpoint_envelope(path: Path) -> dict[str, object]:
    try:
        info = os.lstat(path)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_UNSAFE")
        value = decode_strict_canonical_json(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_INVALID") from exc
    if type(value) is not dict:
        raise _error("EXP012_GATE_C_CHECKPOINT_ENVELOPE_INVALID")
    parse_gate_c_bounded_execution_envelope_document(value)
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operands", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("preflight", "execute", "checkpoint-preflight", "checkpoint-execute"),
        required=True,
    )
    parser.add_argument("--confirm-physical-shadow-searches", action="store_true")
    parser.add_argument("--confirm-bounded-physical-shadow-searches", action="store_true")
    parser.add_argument("--execution-envelope", type=Path)
    parser.add_argument("--start-window-sequence", type=int)
    parser.add_argument("--window-count", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        operands = load_operands(args.operands)
        plan = build_gate_c_plan(operands)
        sys.stdout.write(strict_canonical_json_bytes(plan).decode() + os.linesep)
        if args.mode == "preflight":
            if any(
                item is not None
                for item in (
                    args.execution_envelope,
                    args.start_window_sequence,
                    args.window_count,
                )
            ) or args.confirm_bounded_physical_shadow_searches:
                raise _error("EXP012_GATE_C_MODE_ARGUMENT_CONFLICT")
            return 0
        if args.mode == "execute":
            if any(
                item is not None
                for item in (
                    args.execution_envelope,
                    args.start_window_sequence,
                    args.window_count,
                )
            ) or args.confirm_bounded_physical_shadow_searches:
                raise _error("EXP012_GATE_C_MODE_ARGUMENT_CONFLICT")
            if not args.confirm_physical_shadow_searches:
                raise _error("EXP012_GATE_C_CONFIRMATION_REQUIRED")
            result = _run_gate_c_after_plan(operands, plan)
        elif args.mode == "checkpoint-preflight":
            if (
                args.execution_envelope is None
                or args.start_window_sequence is None
                or args.window_count is None
                or args.confirm_physical_shadow_searches
                or args.confirm_bounded_physical_shadow_searches
            ):
                raise _error("EXP012_GATE_C_CHECKPOINT_ARGUMENT_INVALID")
            envelope = build_gate_c_checkpoint_envelope(
                operands,
                GateCWindowExecutionBound(
                    start_window_sequence=args.start_window_sequence,
                    window_count=args.window_count,
                ),
            )
            _write_checkpoint_envelope(args.execution_envelope, envelope)
            sys.stdout.write(
                strict_canonical_json_bytes(
                    gate_c_bounded_execution_envelope_document(envelope)
                ).decode()
                + os.linesep
            )
            return 0
        else:
            if (
                args.execution_envelope is None
                or args.start_window_sequence is not None
                or args.window_count is not None
                or args.confirm_physical_shadow_searches
                or not args.confirm_bounded_physical_shadow_searches
            ):
                raise _error("EXP012_GATE_C_CHECKPOINT_CONFIRMATION_REQUIRED")
            result = run_gate_c_checkpoint(
                operands, _load_checkpoint_envelope(args.execution_envelope)
            )
        sys.stdout.write(strict_canonical_json_bytes(result).decode() + os.linesep)
        return 0
    except (
        Exp012ScaleGateCOperatorError,
        Exp010GateCOperatorError,
        HostResponseCommitError,
        ShadowSearchTelemetryError,
        GateCBoundedExecutionError,
        GateCWindowExecutionError,
        GateCCheckpointLedgerError,
    ) as exc:
        sys.stderr.write(f"{exc.code}: {exc}{os.linesep}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
