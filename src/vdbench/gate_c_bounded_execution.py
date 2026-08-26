"""Pure contracts for explicitly bounded EXP-012 Gate-C execution.

The two-field window bound is the only execution-range authority.  The
detached envelope binds that range to a freshly reconstructed full Gate-C plan,
campaign marker, and verified source/outbox heads.  This module performs no I/O
and creates no qualification, admission, routing, grant, or actuation authority.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path

from .canonical_serialization import strict_canonical_digest
from .artifacts import canonical_json_bytes
from .exp012_scale_campaign import Exp012ScaleCampaignBinding
from .exp012_scale_contract import (
    Exp012ScaleContract,
    Exp012ScaleProfile,
    build_exp012_scale_contract,
    exp012_scale_contract_payload,
    verify_exp012_scale_contract,
)
from .host_window_lineage import CommittedHostObservation, VerifiedHostSourceHead
from .gate_c_window_execution import (
    GateCWindowExecutionBound,
    GateCWindowExecutionError,
    verify_gate_c_window_execution_bound,
)
from .gate_c_execution_environment import (
    GateCExecutionEnvironmentAttestation,
    gate_c_execution_environment_attestation_document,
    parse_gate_c_execution_environment_attestation_document,
    verify_gate_c_execution_environment_eligibility,
)
from .shadow_window import TRACE_COUNT, WINDOW_QUERY_COUNT

__all__ = [
    "BOUND_SCHEMA_VERSION",
    "CHECKPOINT_RESULT_SCHEMA_VERSION",
    "CHECKPOINT_RESULT_SCHEMA_VERSION_V2",
    "CHECKPOINT_RESULT_SCHEMA_VERSION_V3",
    "ENVELOPE_SCHEMA_VERSION",
    "ENVELOPE_SCHEMA_VERSION_V2",
    "ENVELOPE_SCHEMA_VERSION_V3",
    "GateCBoundedExecutionError",
    "GateCBoundedExecutionEnvelope",
    "GateCBoundedExecutionEnvelopeV2",
    "GateCBoundedExecutionEnvelopeV3",
    "GateCWindowExecutionBound",
    "build_gate_c_bounded_execution_envelope",
    "build_gate_c_bounded_execution_envelope_v2",
    "build_gate_c_bounded_execution_envelope_v3",
    "build_gate_c_canonical_state",
    "build_gate_c_checkpoint_result",
    "build_gate_c_checkpoint_result_v2",
    "build_gate_c_checkpoint_result_v3",
    "build_gate_c_window_checkpoint_effect",
    "derive_gate_c_producer_run_id",
    "gate_c_bounded_execution_envelope_document",
    "gate_c_bounded_execution_envelope_document_v2",
    "gate_c_bounded_execution_envelope_document_v3",
    "gate_c_bounded_execution_envelope_payload",
    "gate_c_bounded_execution_envelope_payload_v2",
    "gate_c_bounded_execution_envelope_payload_v3",
    "parse_gate_c_bounded_execution_envelope_document",
    "parse_gate_c_bounded_execution_envelope_document_v2",
    "parse_gate_c_bounded_execution_envelope_document_v3",
    "verify_gate_c_bounded_execution_envelope",
    "verify_gate_c_bounded_execution_envelope_v2",
    "verify_gate_c_bounded_execution_envelope_v3",
    "verify_gate_c_canonical_state",
    "verify_gate_c_checkpoint_result",
    "verify_gate_c_checkpoint_result_v2",
    "verify_gate_c_checkpoint_result_v3",
    "verify_gate_c_window_checkpoint_effect",
    "verify_gate_c_window_execution_bound",
]


BOUND_SCHEMA_VERSION = "gate-c-window-execution-bound-v1"
ENVELOPE_SCHEMA_VERSION = "exp012-scale-gate-c-bounded-execution-envelope-v1"
CHECKPOINT_RESULT_SCHEMA_VERSION = "exp012-scale-gate-c-checkpoint-result-v1"
ENVELOPE_SCHEMA_VERSION_V2 = "exp012-scale-gate-c-bounded-execution-envelope-v2"
CHECKPOINT_RESULT_SCHEMA_VERSION_V2 = "exp012-scale-gate-c-checkpoint-result-v2"
ENVELOPE_SCHEMA_VERSION_V3 = "exp012-scale-gate-c-bounded-execution-envelope-v3"
CHECKPOINT_RESULT_SCHEMA_VERSION_V3 = "exp012-scale-gate-c-checkpoint-result-v3"
_STATE_SCHEMA_VERSION = "exp012-scale-gate-c-canonical-state-v1"
_EFFECT_SCHEMA_VERSION = "exp012-scale-gate-c-window-effect-v1"
_ENVELOPE_DOMAIN = b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V1\x00"
_RESULT_DOMAIN = b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V1\x00"
_ENVELOPE_DOMAIN_V2 = b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V2\x00"
_RESULT_DOMAIN_V2 = b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V2\x00"
_ENVELOPE_DOMAIN_V3 = b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V3\x00"
_RESULT_DOMAIN_V3 = b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V3\x00"
_STATE_DOMAIN = b"VD::EXP012_SCALE_GATE_C_CANONICAL_STATE::V1\x00"
_EFFECT_DOMAIN = b"VD::EXP012_SCALE_GATE_C_WINDOW_EFFECT::V1\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_REVISION = re.compile(r"[0-9a-f]{40}")
_PRODUCER_QUERY_ID = re.compile(r"logsim-v2:([0-9a-f]{32}):(0|[1-9][0-9]*)")


class GateCBoundedExecutionError(ValueError):
    """Fail-closed bounded-execution error carrying one stable reason code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> GateCBoundedExecutionError:
    return GateCBoundedExecutionError(code)


def _sha(value: object, *, code: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _error(code)
    return value


def _text(value: object, *, code: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _error(code)
    return value


@dataclass(frozen=True, slots=True, init=False)
class GateCBoundedExecutionEnvelope:
    """Builder-issued binding between one full plan and one exact bound."""

    schema_version: str
    plan_sha256: str
    campaign_identity: str
    campaign_binding_sha256: str
    scale_contract: Exp012ScaleContract
    gate_a_evidence_sha256: str
    source_store_binding_sha256: str
    source_head_sha256: str
    outbox_head_sha256: str
    source_head_snapshot_sha256: str
    source_revision: str
    producer_run_id: str
    source_count: int
    metric: str
    threshold_stratum: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    environment_manifest_sha256: str
    execution_bound: GateCWindowExecutionBound
    envelope_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("bounded Gate-C envelopes are builder-issued")


@dataclass(frozen=True, slots=True, init=False)
class GateCBoundedExecutionEnvelopeV2:
    """V2 envelope separating upstream and bounded-executor provenance."""

    schema_version: str
    plan_sha256: str
    campaign_identity: str
    campaign_binding_sha256: str
    scale_contract: Exp012ScaleContract
    gate_a_evidence_sha256: str
    source_store_binding_sha256: str
    source_head_sha256: str
    outbox_head_sha256: str
    source_head_snapshot_sha256: str
    source_revision: str
    execution_source_revision: str
    producer_run_id: str
    source_count: int
    metric: str
    threshold_stratum: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    environment_manifest_sha256: str
    execution_bound: GateCWindowExecutionBound
    envelope_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("bounded Gate-C v2 envelopes are builder-issued")


@dataclass(frozen=True, slots=True, init=False)
class GateCBoundedExecutionEnvelopeV3:
    """V3 envelope binding both executor and current runtime provenance."""

    schema_version: str
    plan_sha256: str
    campaign_identity: str
    campaign_binding_sha256: str
    scale_contract: Exp012ScaleContract
    gate_a_evidence_sha256: str
    source_store_binding_sha256: str
    source_head_sha256: str
    outbox_head_sha256: str
    source_head_snapshot_sha256: str
    source_revision: str
    execution_source_revision: str
    execution_environment_identity_sha256: str
    execution_environment_attestation_sha256: str
    execution_environment_attestation: GateCExecutionEnvironmentAttestation
    producer_run_id: str
    source_count: int
    metric: str
    threshold_stratum: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    environment_manifest_sha256: str
    execution_bound: GateCWindowExecutionBound
    envelope_sha256: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("bounded Gate-C v3 envelopes are builder-issued")


def _make_envelope(**values: object) -> GateCBoundedExecutionEnvelope:
    result = object.__new__(GateCBoundedExecutionEnvelope)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _make_envelope_v2(**values: object) -> GateCBoundedExecutionEnvelopeV2:
    result = object.__new__(GateCBoundedExecutionEnvelopeV2)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _make_envelope_v3(**values: object) -> GateCBoundedExecutionEnvelopeV3:
    result = object.__new__(GateCBoundedExecutionEnvelopeV3)
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def _plan_payload(plan: Mapping[str, object]) -> dict[str, object]:
    if type(plan) is not dict or type(plan.get("plan_sha256")) is not str:
        raise _error("GATE_C_BOUNDED_PLAN_INVALID")
    payload = dict(plan)
    claimed = payload.pop("plan_sha256")
    if strict_canonical_digest(b"VD::EXP012_SCALE_GATE_C_PLAN::V1\x00", payload) != claimed:
        raise _error("GATE_C_BOUNDED_PLAN_INVALID")
    if payload.get("schema_version") != "exp012-scale-gate-c-plan-v1":
        raise _error("GATE_C_BOUNDED_PLAN_INVALID")
    return payload


def derive_gate_c_producer_run_id(
    sources: tuple[CommittedHostObservation, ...],
) -> str:
    """Derive one producer id from the exact canonical source sequence."""

    if type(sources) is not tuple or not sources:
        raise _error("GATE_C_PRODUCER_IDENTITY_INVALID")
    producer: str | None = None
    for expected, source in enumerate(sources):
        if type(source) is not CommittedHostObservation or source.source_sequence != expected:
            raise _error("GATE_C_PRODUCER_IDENTITY_INVALID")
        query_id = source.query_id
        if type(query_id) is not str:
            raise _error("GATE_C_PRODUCER_IDENTITY_INVALID")
        match = _PRODUCER_QUERY_ID.fullmatch(query_id)
        if match is None or int(match.group(2)) != expected:
            raise _error("GATE_C_PRODUCER_IDENTITY_INVALID")
        if producer is None:
            producer = match.group(1)
        elif producer != match.group(1):
            raise _error("GATE_C_PRODUCER_IDENTITY_INVALID")
    if producer is None:
        raise _error("GATE_C_PRODUCER_IDENTITY_INVALID")
    return producer


def _extract_plan_values(plan: Mapping[str, object]) -> dict[str, object]:
    try:
        stream = plan["stream"]
        observed = plan["observed"]
        gate_a = plan["gate_a_authority"]
        stores = plan["stores"]
        if not all(type(item) is dict for item in (stream, observed, gate_a, stores)):
            raise TypeError
        return {
            "plan_sha256": plan["plan_sha256"],
            "campaign_identity": Path(stores["root"]).parent.name,
            "scale_contract": plan["scale_contract"],
            "scale_contract_sha256": plan["scale_contract_sha256"],
            "gate_a_evidence_sha256": gate_a["evidence_sha256"],
            "source_revision": plan["source_revision"],
            "source_count": observed["source_count"],
            "complete_source_windows": observed["complete_source_windows"],
            "next_window_sequence": observed["next_window_sequence"],
            "metric": stream["metric"],
            "threshold_stratum": stream["threshold_stratum"],
            "configuration_identity": stream["configuration_identity"],
            "data_identity": stream["data_identity"],
            "flat_binding_id": stream["flat_binding_id"],
            "hnsw_binding_id": stream["hnsw_binding_id"],
            "environment_manifest_sha256": plan["environment_manifest_sha256"],
        }
    except (KeyError, TypeError, AttributeError) as exc:
        raise _error("GATE_C_BOUNDED_PLAN_INVALID") from exc


def _envelope_values(
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
    execution_bound: GateCWindowExecutionBound,
) -> dict[str, object]:
    _plan_payload(plan)
    values = _extract_plan_values(plan)
    try:
        bound = verify_gate_c_window_execution_bound(execution_bound)
    except GateCWindowExecutionError as exc:
        raise _error("GATE_C_EXECUTION_BOUND_INVALID") from exc
    if type(campaign_binding) is not Exp012ScaleCampaignBinding:
        raise _error("GATE_C_CAMPAIGN_BINDING_INVALID")
    if type(source_head) is not VerifiedHostSourceHead:
        raise _error("GATE_C_SOURCE_HEAD_INVALID")
    try:
        contract = verify_exp012_scale_contract(campaign_binding.contract)
    except (TypeError, ValueError) as exc:
        raise _error("GATE_C_CAMPAIGN_BINDING_INVALID") from exc
    campaign_payload = {
        "schema_version": "exp012-scale-campaign-v1",
        "experiment_id": "EXP-012-SCALE",
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "gate_a_evidence_sha256": campaign_binding.gate_a_evidence_sha256,
    }
    expected_campaign_sha256 = strict_canonical_digest(
        b"VD::EXP012_SCALE_CAMPAIGN::V1\x00", campaign_payload
    )
    source_head_payload = {
        "schema_version": "response-profile-host-verified-head-v1",
        "source_count": source_head.source_count,
        "maximum_source_sequence": source_head.maximum_source_sequence,
        "source_head_sha256": source_head.source_head_sha256,
        "outbox_head_sha256": source_head.outbox_head_sha256,
        "store_binding_sha256": source_head.store_binding_sha256,
    }
    expected_head_snapshot_sha256 = hashlib.sha256(
        b"VD::HOST_RESPONSE_VERIFIED_HEAD::V1\x00"
        + canonical_json_bytes(source_head_payload)
    ).hexdigest()
    if (
        values["scale_contract"] != exp012_scale_contract_payload(contract)
        or values["scale_contract_sha256"] != contract.contract_sha256
        or values["gate_a_evidence_sha256"] != campaign_binding.gate_a_evidence_sha256
        or values["source_count"] != contract.target_source_records
        or values["source_count"] != source_head.source_count
        or source_head.maximum_source_sequence != source_head.source_count - 1
        or values["complete_source_windows"] != contract.expected_windows
        or values["next_window_sequence"] != bound.start_window_sequence
        or bound.expected_next_window_sequence > contract.expected_windows
        or len(sources) != source_head.source_count
        or campaign_binding.campaign_sha256 != expected_campaign_sha256
        or source_head.head_snapshot_sha256 != expected_head_snapshot_sha256
    ):
        raise _error("GATE_C_BOUNDED_AUTHORITY_MISMATCH")
    producer_run_id = derive_gate_c_producer_run_id(sources)
    for source in sources:
        stream = source.stream_key
        if (
            source.source_revision != values["source_revision"]
            or source.environment_manifest_sha256
            != values["environment_manifest_sha256"]
            or stream.metric.value != values["metric"]
            or stream.threshold_stratum != values["threshold_stratum"]
            or stream.configuration_identity != values["configuration_identity"]
            or stream.data_identity != values["data_identity"]
            or stream.flat_binding_id != values["flat_binding_id"]
            or stream.hnsw_binding_id != values["hnsw_binding_id"]
        ):
            raise _error("GATE_C_BOUNDED_SOURCE_IDENTITY_MISMATCH")
    if sources[-1].source_sha256 != source_head.source_head_sha256:
        raise _error("GATE_C_BOUNDED_SOURCE_IDENTITY_MISMATCH")
    return {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "plan_sha256": _sha(values["plan_sha256"], code="GATE_C_BOUNDED_PLAN_INVALID"),
        "campaign_identity": _text(values["campaign_identity"], code="GATE_C_CAMPAIGN_BINDING_INVALID"),
        "campaign_binding_sha256": _sha(campaign_binding.campaign_sha256, code="GATE_C_CAMPAIGN_BINDING_INVALID"),
        "scale_contract": contract,
        "gate_a_evidence_sha256": _sha(campaign_binding.gate_a_evidence_sha256, code="GATE_C_CAMPAIGN_BINDING_INVALID"),
        "source_store_binding_sha256": _sha(source_head.store_binding_sha256, code="GATE_C_SOURCE_HEAD_INVALID"),
        "source_head_sha256": _sha(source_head.source_head_sha256, code="GATE_C_SOURCE_HEAD_INVALID"),
        "outbox_head_sha256": _sha(source_head.outbox_head_sha256, code="GATE_C_SOURCE_HEAD_INVALID"),
        "source_head_snapshot_sha256": _sha(source_head.head_snapshot_sha256, code="GATE_C_SOURCE_HEAD_INVALID"),
        "source_revision": _text(values["source_revision"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "producer_run_id": producer_run_id,
        "source_count": source_head.source_count,
        "metric": _text(values["metric"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "threshold_stratum": _text(values["threshold_stratum"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "configuration_identity": _text(values["configuration_identity"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "data_identity": _text(values["data_identity"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "flat_binding_id": _text(values["flat_binding_id"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "hnsw_binding_id": _text(values["hnsw_binding_id"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "environment_manifest_sha256": _sha(values["environment_manifest_sha256"], code="GATE_C_BOUNDED_AUTHORITY_MISMATCH"),
        "execution_bound": bound,
    }


def gate_c_bounded_execution_envelope_payload(
    envelope: GateCBoundedExecutionEnvelope,
) -> dict[str, object]:
    if type(envelope) is not GateCBoundedExecutionEnvelope:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    try:
        bound = verify_gate_c_window_execution_bound(envelope.execution_bound)
        contract = verify_exp012_scale_contract(envelope.scale_contract)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID") from exc
    # The governed scale contract must reject an oversized derived end before
    # the allowed sequence tuple/list below is materialized. This also covers
    # exact-type envelopes forged by bypassing the private builder.
    if bound.expected_next_window_sequence > contract.expected_windows:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    return {
        "schema_version": envelope.schema_version,
        "plan_sha256": envelope.plan_sha256,
        "campaign_identity": envelope.campaign_identity,
        "campaign_binding_sha256": envelope.campaign_binding_sha256,
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "gate_a_evidence_sha256": envelope.gate_a_evidence_sha256,
        "source_store_binding_sha256": envelope.source_store_binding_sha256,
        "source_head_sha256": envelope.source_head_sha256,
        "outbox_head_sha256": envelope.outbox_head_sha256,
        "source_head_snapshot_sha256": envelope.source_head_snapshot_sha256,
        "source_revision": envelope.source_revision,
        "producer_run_id": envelope.producer_run_id,
        "source_count": envelope.source_count,
        "metric": envelope.metric,
        "threshold_stratum": envelope.threshold_stratum,
        "configuration_identity": envelope.configuration_identity,
        "data_identity": envelope.data_identity,
        "flat_binding_id": envelope.flat_binding_id,
        "hnsw_binding_id": envelope.hnsw_binding_id,
        "environment_manifest_sha256": envelope.environment_manifest_sha256,
        "execution_bound": {
            "schema_version": BOUND_SCHEMA_VERSION,
            "start_window_sequence": bound.start_window_sequence,
            "window_count": bound.window_count,
            "allowed_window_sequences": list(bound.allowed_window_sequences),
            "expected_next_window_sequence": bound.expected_next_window_sequence,
        },
    }


def build_gate_c_bounded_execution_envelope(
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
    execution_bound: GateCWindowExecutionBound,
) -> GateCBoundedExecutionEnvelope:
    values = _envelope_values(
        plan=plan,
        campaign_binding=campaign_binding,
        source_head=source_head,
        sources=sources,
        execution_bound=execution_bound,
    )
    provisional = _make_envelope(**values, envelope_sha256="")
    payload = gate_c_bounded_execution_envelope_payload(provisional)
    return _make_envelope(
        **values, envelope_sha256=strict_canonical_digest(_ENVELOPE_DOMAIN, payload)
    )


def gate_c_bounded_execution_envelope_document(
    envelope: GateCBoundedExecutionEnvelope,
) -> dict[str, object]:
    payload = gate_c_bounded_execution_envelope_payload(envelope)
    digest = strict_canonical_digest(_ENVELOPE_DOMAIN, payload)
    if envelope.envelope_sha256 != digest:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    return {"envelope_payload": payload, "envelope_sha256": digest}


def _bound_from_payload(
    value: object, *, maximum_expected_next_window_sequence: int
) -> GateCWindowExecutionBound:
    if type(value) is not dict or set(value) != {
        "schema_version", "start_window_sequence", "window_count",
        "allowed_window_sequences", "expected_next_window_sequence",
    }:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    if value["schema_version"] != BOUND_SCHEMA_VERSION:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    bound = GateCWindowExecutionBound(
        start_window_sequence=value["start_window_sequence"],
        window_count=value["window_count"],
    )
    if bound.expected_next_window_sequence > maximum_expected_next_window_sequence:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    if (
        value["allowed_window_sequences"] != list(bound.allowed_window_sequences)
        or value["expected_next_window_sequence"] != bound.expected_next_window_sequence
    ):
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    return bound


def _envelope_from_document(document: Mapping[str, object]) -> GateCBoundedExecutionEnvelope:
    if type(document) is not dict or set(document) != {"envelope_payload", "envelope_sha256"}:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    payload = document["envelope_payload"]
    expected_fields = {
        "schema_version", "plan_sha256", "campaign_identity",
        "campaign_binding_sha256", "scale_contract", "scale_contract_sha256",
        "gate_a_evidence_sha256", "source_store_binding_sha256",
        "source_head_sha256", "outbox_head_sha256", "source_head_snapshot_sha256",
        "source_revision", "producer_run_id", "source_count", "metric",
        "threshold_stratum", "configuration_identity", "data_identity",
        "flat_binding_id", "hnsw_binding_id", "environment_manifest_sha256",
        "execution_bound",
    }
    if type(payload) is not dict or set(payload) != expected_fields:
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID")
    try:
        contract_payload = payload["scale_contract"]
        if type(contract_payload) is not dict:
            raise TypeError
        contract = build_exp012_scale_contract(Exp012ScaleProfile(contract_payload["profile"]))
        if (
            contract_payload != exp012_scale_contract_payload(contract)
            or payload["scale_contract_sha256"] != contract.contract_sha256
        ):
            raise ValueError
        bound = _bound_from_payload(
            payload["execution_bound"],
            maximum_expected_next_window_sequence=contract.expected_windows,
        )
        if (
            payload["schema_version"] != ENVELOPE_SCHEMA_VERSION
            or type(payload["source_count"]) is not int
            or payload["source_count"] <= 0
            or bound.expected_next_window_sequence > contract.expected_windows
            or payload["source_count"] != contract.target_source_records
            or _PRODUCER_QUERY_ID.fullmatch(
                f"logsim-v2:{payload['producer_run_id']}:0"
            ) is None
        ):
            raise ValueError
        for name in (
            "plan_sha256", "campaign_binding_sha256", "gate_a_evidence_sha256",
            "source_store_binding_sha256", "source_head_sha256",
            "outbox_head_sha256", "source_head_snapshot_sha256",
            "environment_manifest_sha256",
        ):
            _sha(payload[name], code="GATE_C_BOUNDED_ENVELOPE_INVALID")
        for name in (
            "campaign_identity", "source_revision", "producer_run_id", "metric",
            "threshold_stratum", "configuration_identity", "data_identity",
            "flat_binding_id", "hnsw_binding_id",
        ):
            _text(payload[name], code="GATE_C_BOUNDED_ENVELOPE_INVALID")
        campaign_payload = {
            "schema_version": "exp012-scale-campaign-v1",
            "experiment_id": "EXP-012-SCALE",
            "scale_contract": exp012_scale_contract_payload(contract),
            "scale_contract_sha256": contract.contract_sha256,
            "gate_a_evidence_sha256": payload["gate_a_evidence_sha256"],
        }
        if payload["campaign_binding_sha256"] != strict_canonical_digest(
            b"VD::EXP012_SCALE_CAMPAIGN::V1\x00", campaign_payload
        ):
            raise ValueError
        source_head_payload = {
            "schema_version": "response-profile-host-verified-head-v1",
            "source_count": payload["source_count"],
            "maximum_source_sequence": payload["source_count"] - 1,
            "source_head_sha256": payload["source_head_sha256"],
            "outbox_head_sha256": payload["outbox_head_sha256"],
            "store_binding_sha256": payload["source_store_binding_sha256"],
        }
        if payload["source_head_snapshot_sha256"] != hashlib.sha256(
            b"VD::HOST_RESPONSE_VERIFIED_HEAD::V1\x00"
            + canonical_json_bytes(source_head_payload)
        ).hexdigest():
            raise ValueError
        envelope = _make_envelope(
            schema_version=payload["schema_version"],
            plan_sha256=payload["plan_sha256"],
            campaign_identity=payload["campaign_identity"],
            campaign_binding_sha256=payload["campaign_binding_sha256"],
            scale_contract=contract,
            gate_a_evidence_sha256=payload["gate_a_evidence_sha256"],
            source_store_binding_sha256=payload["source_store_binding_sha256"],
            source_head_sha256=payload["source_head_sha256"],
            outbox_head_sha256=payload["outbox_head_sha256"],
            source_head_snapshot_sha256=payload["source_head_snapshot_sha256"],
            source_revision=payload["source_revision"],
            producer_run_id=payload["producer_run_id"],
            source_count=payload["source_count"],
            metric=payload["metric"],
            threshold_stratum=payload["threshold_stratum"],
            configuration_identity=payload["configuration_identity"],
            data_identity=payload["data_identity"],
            flat_binding_id=payload["flat_binding_id"],
            hnsw_binding_id=payload["hnsw_binding_id"],
            environment_manifest_sha256=payload["environment_manifest_sha256"],
            execution_bound=bound,
            envelope_sha256=document["envelope_sha256"],
        )
        gate_c_bounded_execution_envelope_document(envelope)
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_BOUNDED_ENVELOPE_INVALID") from exc
    return envelope


def parse_gate_c_bounded_execution_envelope_document(
    document: Mapping[str, object],
) -> GateCBoundedExecutionEnvelope:
    """Reconstruct one self-consistent envelope without asserting live freshness."""

    return _envelope_from_document(document)


def verify_gate_c_bounded_execution_envelope(
    document: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
) -> GateCBoundedExecutionEnvelope:
    supplied = _envelope_from_document(document)
    expected = build_gate_c_bounded_execution_envelope(
        plan=plan,
        campaign_binding=campaign_binding,
        source_head=source_head,
        sources=sources,
        execution_bound=supplied.execution_bound,
    )
    if any(
        type(getattr(supplied, item.name)) is not type(getattr(expected, item.name))
        or getattr(supplied, item.name) != getattr(expected, item.name)
        for item in fields(GateCBoundedExecutionEnvelope)
    ):
        raise _error("GATE_C_BOUNDED_ENVELOPE_MISMATCH")
    return expected


def gate_c_bounded_execution_envelope_payload_v2(
    envelope: GateCBoundedExecutionEnvelopeV2,
) -> dict[str, object]:
    if type(envelope) is not GateCBoundedExecutionEnvelopeV2:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
    try:
        bound = verify_gate_c_window_execution_bound(envelope.execution_bound)
        contract = verify_exp012_scale_contract(envelope.scale_contract)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID") from exc
    if (
        bound.expected_next_window_sequence > contract.expected_windows
        or type(envelope.execution_source_revision) is not str
        or _REVISION.fullmatch(envelope.execution_source_revision) is None
    ):
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
    return {
        "schema_version": envelope.schema_version,
        "plan_sha256": envelope.plan_sha256,
        "campaign_identity": envelope.campaign_identity,
        "campaign_binding_sha256": envelope.campaign_binding_sha256,
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "gate_a_evidence_sha256": envelope.gate_a_evidence_sha256,
        "source_store_binding_sha256": envelope.source_store_binding_sha256,
        "source_head_sha256": envelope.source_head_sha256,
        "outbox_head_sha256": envelope.outbox_head_sha256,
        "source_head_snapshot_sha256": envelope.source_head_snapshot_sha256,
        "source_revision": envelope.source_revision,
        "execution_source_revision": envelope.execution_source_revision,
        "producer_run_id": envelope.producer_run_id,
        "source_count": envelope.source_count,
        "metric": envelope.metric,
        "threshold_stratum": envelope.threshold_stratum,
        "configuration_identity": envelope.configuration_identity,
        "data_identity": envelope.data_identity,
        "flat_binding_id": envelope.flat_binding_id,
        "hnsw_binding_id": envelope.hnsw_binding_id,
        "environment_manifest_sha256": envelope.environment_manifest_sha256,
        "execution_bound": {
            "schema_version": BOUND_SCHEMA_VERSION,
            "start_window_sequence": bound.start_window_sequence,
            "window_count": bound.window_count,
            "allowed_window_sequences": list(bound.allowed_window_sequences),
            "expected_next_window_sequence": bound.expected_next_window_sequence,
        },
    }


def build_gate_c_bounded_execution_envelope_v2(
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
    execution_bound: GateCWindowExecutionBound,
    execution_source_revision: str,
) -> GateCBoundedExecutionEnvelopeV2:
    values = _envelope_values(
        plan=plan,
        campaign_binding=campaign_binding,
        source_head=source_head,
        sources=sources,
        execution_bound=execution_bound,
    )
    if (
        type(execution_source_revision) is not str
        or _REVISION.fullmatch(execution_source_revision) is None
    ):
        raise _error("GATE_C_EXECUTION_SOURCE_REVISION_INVALID")
    values["schema_version"] = ENVELOPE_SCHEMA_VERSION_V2
    values["execution_source_revision"] = execution_source_revision
    provisional = _make_envelope_v2(**values, envelope_sha256="")
    payload = gate_c_bounded_execution_envelope_payload_v2(provisional)
    return _make_envelope_v2(
        **values,
        envelope_sha256=strict_canonical_digest(_ENVELOPE_DOMAIN_V2, payload),
    )


def gate_c_bounded_execution_envelope_document_v2(
    envelope: GateCBoundedExecutionEnvelopeV2,
) -> dict[str, object]:
    payload = gate_c_bounded_execution_envelope_payload_v2(envelope)
    digest = strict_canonical_digest(_ENVELOPE_DOMAIN_V2, payload)
    if envelope.envelope_sha256 != digest:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
    return {"envelope_payload": payload, "envelope_sha256": digest}


def _envelope_v2_from_document(
    document: Mapping[str, object],
) -> GateCBoundedExecutionEnvelopeV2:
    if type(document) is not dict or set(document) != {
        "envelope_payload", "envelope_sha256"
    }:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
    payload = document["envelope_payload"]
    expected_fields = {
        "schema_version", "plan_sha256", "campaign_identity",
        "campaign_binding_sha256", "scale_contract", "scale_contract_sha256",
        "gate_a_evidence_sha256", "source_store_binding_sha256",
        "source_head_sha256", "outbox_head_sha256", "source_head_snapshot_sha256",
        "source_revision", "execution_source_revision", "producer_run_id",
        "source_count", "metric", "threshold_stratum", "configuration_identity",
        "data_identity", "flat_binding_id", "hnsw_binding_id",
        "environment_manifest_sha256", "execution_bound",
    }
    if type(payload) is not dict or set(payload) != expected_fields:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
    try:
        contract_payload = payload["scale_contract"]
        if type(contract_payload) is not dict:
            raise TypeError
        contract = build_exp012_scale_contract(
            Exp012ScaleProfile(contract_payload["profile"])
        )
        if (
            contract_payload != exp012_scale_contract_payload(contract)
            or payload["scale_contract_sha256"] != contract.contract_sha256
        ):
            raise ValueError
        bound = _bound_from_payload(
            payload["execution_bound"],
            maximum_expected_next_window_sequence=contract.expected_windows,
        )
        if (
            payload["schema_version"] != ENVELOPE_SCHEMA_VERSION_V2
            or type(payload["source_count"]) is not int
            or payload["source_count"] != contract.target_source_records
            or _PRODUCER_QUERY_ID.fullmatch(
                f"logsim-v2:{payload['producer_run_id']}:0"
            ) is None
            or _REVISION.fullmatch(payload["execution_source_revision"]) is None
        ):
            raise ValueError
        for name in (
            "plan_sha256", "campaign_binding_sha256", "gate_a_evidence_sha256",
            "source_store_binding_sha256", "source_head_sha256",
            "outbox_head_sha256", "source_head_snapshot_sha256",
            "environment_manifest_sha256",
        ):
            _sha(payload[name], code="GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
        for name in (
            "campaign_identity", "source_revision", "execution_source_revision",
            "producer_run_id", "metric", "threshold_stratum",
            "configuration_identity", "data_identity", "flat_binding_id",
            "hnsw_binding_id",
        ):
            _text(payload[name], code="GATE_C_BOUNDED_ENVELOPE_V2_INVALID")
        campaign_payload = {
            "schema_version": "exp012-scale-campaign-v1",
            "experiment_id": "EXP-012-SCALE",
            "scale_contract": exp012_scale_contract_payload(contract),
            "scale_contract_sha256": contract.contract_sha256,
            "gate_a_evidence_sha256": payload["gate_a_evidence_sha256"],
        }
        if payload["campaign_binding_sha256"] != strict_canonical_digest(
            b"VD::EXP012_SCALE_CAMPAIGN::V1\x00", campaign_payload
        ):
            raise ValueError
        source_head_payload = {
            "schema_version": "response-profile-host-verified-head-v1",
            "source_count": payload["source_count"],
            "maximum_source_sequence": payload["source_count"] - 1,
            "source_head_sha256": payload["source_head_sha256"],
            "outbox_head_sha256": payload["outbox_head_sha256"],
            "store_binding_sha256": payload["source_store_binding_sha256"],
        }
        if payload["source_head_snapshot_sha256"] != hashlib.sha256(
            b"VD::HOST_RESPONSE_VERIFIED_HEAD::V1\x00"
            + canonical_json_bytes(source_head_payload)
        ).hexdigest():
            raise ValueError
        envelope = _make_envelope_v2(
            schema_version=payload["schema_version"],
            plan_sha256=payload["plan_sha256"],
            campaign_identity=payload["campaign_identity"],
            campaign_binding_sha256=payload["campaign_binding_sha256"],
            scale_contract=contract,
            gate_a_evidence_sha256=payload["gate_a_evidence_sha256"],
            source_store_binding_sha256=payload["source_store_binding_sha256"],
            source_head_sha256=payload["source_head_sha256"],
            outbox_head_sha256=payload["outbox_head_sha256"],
            source_head_snapshot_sha256=payload["source_head_snapshot_sha256"],
            source_revision=payload["source_revision"],
            execution_source_revision=payload["execution_source_revision"],
            producer_run_id=payload["producer_run_id"],
            source_count=payload["source_count"],
            metric=payload["metric"],
            threshold_stratum=payload["threshold_stratum"],
            configuration_identity=payload["configuration_identity"],
            data_identity=payload["data_identity"],
            flat_binding_id=payload["flat_binding_id"],
            hnsw_binding_id=payload["hnsw_binding_id"],
            environment_manifest_sha256=payload["environment_manifest_sha256"],
            execution_bound=bound,
            envelope_sha256=document["envelope_sha256"],
        )
        gate_c_bounded_execution_envelope_document_v2(envelope)
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_INVALID") from exc
    return envelope


def parse_gate_c_bounded_execution_envelope_document_v2(
    document: Mapping[str, object],
) -> GateCBoundedExecutionEnvelopeV2:
    return _envelope_v2_from_document(document)


def verify_gate_c_bounded_execution_envelope_v2(
    document: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
    execution_source_revision: str,
) -> GateCBoundedExecutionEnvelopeV2:
    supplied = _envelope_v2_from_document(document)
    expected = build_gate_c_bounded_execution_envelope_v2(
        plan=plan,
        campaign_binding=campaign_binding,
        source_head=source_head,
        sources=sources,
        execution_bound=supplied.execution_bound,
        execution_source_revision=execution_source_revision,
    )
    if any(
        type(getattr(supplied, item.name)) is not type(getattr(expected, item.name))
        or getattr(supplied, item.name) != getattr(expected, item.name)
        for item in fields(GateCBoundedExecutionEnvelopeV2)
    ):
        raise _error("GATE_C_BOUNDED_ENVELOPE_V2_MISMATCH")
    return expected


def gate_c_bounded_execution_envelope_payload_v3(
    envelope: GateCBoundedExecutionEnvelopeV3,
) -> dict[str, object]:
    if type(envelope) is not GateCBoundedExecutionEnvelopeV3:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V3_INVALID")
    try:
        bound = verify_gate_c_window_execution_bound(envelope.execution_bound)
        contract = verify_exp012_scale_contract(envelope.scale_contract)
        attestation_document = gate_c_execution_environment_attestation_document(
            envelope.execution_environment_attestation
        )
        identity = verify_gate_c_execution_environment_eligibility(
            envelope.execution_environment_attestation
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V3_INVALID") from exc
    if (
        bound.expected_next_window_sequence > contract.expected_windows
        or type(envelope.execution_source_revision) is not str
        or _REVISION.fullmatch(envelope.execution_source_revision) is None
        or envelope.execution_environment_identity_sha256
        != identity.execution_environment_identity_sha256
        or envelope.execution_environment_attestation_sha256
        != envelope.execution_environment_attestation.execution_environment_attestation_sha256
        or envelope.execution_source_revision
        != envelope.execution_environment_attestation.execution_source_revision
    ):
        raise _error("GATE_C_BOUNDED_ENVELOPE_V3_INVALID")
    return {
        "schema_version": envelope.schema_version,
        "plan_sha256": envelope.plan_sha256,
        "campaign_identity": envelope.campaign_identity,
        "campaign_binding_sha256": envelope.campaign_binding_sha256,
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "gate_a_evidence_sha256": envelope.gate_a_evidence_sha256,
        "source_store_binding_sha256": envelope.source_store_binding_sha256,
        "source_head_sha256": envelope.source_head_sha256,
        "outbox_head_sha256": envelope.outbox_head_sha256,
        "source_head_snapshot_sha256": envelope.source_head_snapshot_sha256,
        "source_revision": envelope.source_revision,
        "execution_source_revision": envelope.execution_source_revision,
        "execution_environment_identity_sha256": (
            envelope.execution_environment_identity_sha256
        ),
        "execution_environment_attestation_sha256": (
            envelope.execution_environment_attestation_sha256
        ),
        "execution_environment_attestation": attestation_document,
        "producer_run_id": envelope.producer_run_id,
        "source_count": envelope.source_count,
        "metric": envelope.metric,
        "threshold_stratum": envelope.threshold_stratum,
        "configuration_identity": envelope.configuration_identity,
        "data_identity": envelope.data_identity,
        "flat_binding_id": envelope.flat_binding_id,
        "hnsw_binding_id": envelope.hnsw_binding_id,
        "environment_manifest_sha256": envelope.environment_manifest_sha256,
        "execution_bound": {
            "schema_version": BOUND_SCHEMA_VERSION,
            "start_window_sequence": bound.start_window_sequence,
            "window_count": bound.window_count,
            "allowed_window_sequences": list(bound.allowed_window_sequences),
            "expected_next_window_sequence": bound.expected_next_window_sequence,
        },
    }


def build_gate_c_bounded_execution_envelope_v3(
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
    execution_bound: GateCWindowExecutionBound,
    execution_source_revision: str,
    execution_environment_attestation: GateCExecutionEnvironmentAttestation,
) -> GateCBoundedExecutionEnvelopeV3:
    values = _envelope_values(
        plan=plan,
        campaign_binding=campaign_binding,
        source_head=source_head,
        sources=sources,
        execution_bound=execution_bound,
    )
    if (
        type(execution_source_revision) is not str
        or _REVISION.fullmatch(execution_source_revision) is None
        or type(execution_environment_attestation)
        is not GateCExecutionEnvironmentAttestation
    ):
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_AUTHORITY_INVALID")
    try:
        identity = verify_gate_c_execution_environment_eligibility(
            execution_environment_attestation
        )
    except ValueError as exc:
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_AUTHORITY_INVALID") from exc
    if execution_environment_attestation.execution_source_revision != execution_source_revision:
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_AUTHORITY_MISMATCH")
    attestation_payload = execution_environment_attestation.payload
    governed = attestation_payload["governed_bindings"]
    expected_bindings = {
        "campaign_identity": values["campaign_identity"],
        "scale_contract_sha256": campaign_binding.contract.contract_sha256,
        "gate_a_evidence_sha256": values["gate_a_evidence_sha256"],
        "source_revision": values["source_revision"],
        "environment_manifest_sha256": values["environment_manifest_sha256"],
        "data_identity": values["data_identity"],
        "configuration_identity": values["configuration_identity"],
        "flat_binding_id": values["flat_binding_id"],
        "hnsw_binding_id": values["hnsw_binding_id"],
        "metric": values["metric"],
        "dimensions": attestation_payload["execution_environment_identity"][
            "identity_payload"
        ]["data_plane"][0]["dimensions"],
        "expected_entity_count": attestation_payload[
            "execution_environment_identity"
        ]["identity_payload"]["data_plane"][0]["entity_count"],
    }
    if any(governed.get(name) != expected for name, expected in expected_bindings.items()):
        raise _error("GATE_C_EXECUTION_ENVIRONMENT_AUTHORITY_MISMATCH")
    values["schema_version"] = ENVELOPE_SCHEMA_VERSION_V3
    values["execution_source_revision"] = execution_source_revision
    values["execution_environment_identity_sha256"] = (
        identity.execution_environment_identity_sha256
    )
    values["execution_environment_attestation_sha256"] = (
        execution_environment_attestation.execution_environment_attestation_sha256
    )
    values["execution_environment_attestation"] = execution_environment_attestation
    provisional = _make_envelope_v3(**values, envelope_sha256="")
    payload = gate_c_bounded_execution_envelope_payload_v3(provisional)
    return _make_envelope_v3(
        **values,
        envelope_sha256=strict_canonical_digest(_ENVELOPE_DOMAIN_V3, payload),
    )


def gate_c_bounded_execution_envelope_document_v3(
    envelope: GateCBoundedExecutionEnvelopeV3,
) -> dict[str, object]:
    payload = gate_c_bounded_execution_envelope_payload_v3(envelope)
    digest = strict_canonical_digest(_ENVELOPE_DOMAIN_V3, payload)
    if envelope.envelope_sha256 != digest:
        raise _error("GATE_C_BOUNDED_ENVELOPE_V3_INVALID")
    return {"envelope_payload": payload, "envelope_sha256": digest}


def _envelope_v3_from_document(
    document: Mapping[str, object],
) -> GateCBoundedExecutionEnvelopeV3:
    code = "GATE_C_BOUNDED_ENVELOPE_V3_INVALID"
    if type(document) is not dict or set(document) != {
        "envelope_payload", "envelope_sha256"
    }:
        raise _error(code)
    payload = document["envelope_payload"]
    expected_fields = {
        "schema_version", "plan_sha256", "campaign_identity",
        "campaign_binding_sha256", "scale_contract", "scale_contract_sha256",
        "gate_a_evidence_sha256", "source_store_binding_sha256",
        "source_head_sha256", "outbox_head_sha256", "source_head_snapshot_sha256",
        "source_revision", "execution_source_revision",
        "execution_environment_identity_sha256",
        "execution_environment_attestation_sha256",
        "execution_environment_attestation", "producer_run_id", "source_count",
        "metric", "threshold_stratum", "configuration_identity", "data_identity",
        "flat_binding_id", "hnsw_binding_id", "environment_manifest_sha256",
        "execution_bound",
    }
    if type(payload) is not dict or set(payload) != expected_fields:
        raise _error(code)
    try:
        contract_payload = payload["scale_contract"]
        if type(contract_payload) is not dict:
            raise TypeError
        contract = build_exp012_scale_contract(
            Exp012ScaleProfile(contract_payload["profile"])
        )
        if (
            contract_payload != exp012_scale_contract_payload(contract)
            or payload["scale_contract_sha256"] != contract.contract_sha256
            or payload["schema_version"] != ENVELOPE_SCHEMA_VERSION_V3
            or type(payload["source_count"]) is not int
            or payload["source_count"] != contract.target_source_records
        ):
            raise ValueError
        bound = _bound_from_payload(
            payload["execution_bound"],
            maximum_expected_next_window_sequence=contract.expected_windows,
        )
        attestation = parse_gate_c_execution_environment_attestation_document(
            payload["execution_environment_attestation"]
        )
        for name in (
            "plan_sha256", "campaign_binding_sha256", "gate_a_evidence_sha256",
            "source_store_binding_sha256", "source_head_sha256",
            "outbox_head_sha256", "source_head_snapshot_sha256",
            "environment_manifest_sha256", "execution_environment_identity_sha256",
            "execution_environment_attestation_sha256",
        ):
            _sha(payload[name], code=code)
        for name in (
            "campaign_identity", "source_revision", "execution_source_revision",
            "producer_run_id", "metric", "threshold_stratum",
            "configuration_identity", "data_identity", "flat_binding_id",
            "hnsw_binding_id",
        ):
            _text(payload[name], code=code)
        if (
            _REVISION.fullmatch(payload["execution_source_revision"]) is None
            or payload["execution_environment_identity_sha256"]
            != attestation.execution_environment_identity_sha256
            or payload["execution_environment_attestation_sha256"]
            != attestation.execution_environment_attestation_sha256
            or payload["execution_source_revision"]
            != attestation.execution_source_revision
        ):
            raise ValueError
        envelope = _make_envelope_v3(
            schema_version=payload["schema_version"],
            plan_sha256=payload["plan_sha256"],
            campaign_identity=payload["campaign_identity"],
            campaign_binding_sha256=payload["campaign_binding_sha256"],
            scale_contract=contract,
            gate_a_evidence_sha256=payload["gate_a_evidence_sha256"],
            source_store_binding_sha256=payload["source_store_binding_sha256"],
            source_head_sha256=payload["source_head_sha256"],
            outbox_head_sha256=payload["outbox_head_sha256"],
            source_head_snapshot_sha256=payload["source_head_snapshot_sha256"],
            source_revision=payload["source_revision"],
            execution_source_revision=payload["execution_source_revision"],
            execution_environment_identity_sha256=payload[
                "execution_environment_identity_sha256"
            ],
            execution_environment_attestation_sha256=payload[
                "execution_environment_attestation_sha256"
            ],
            execution_environment_attestation=attestation,
            producer_run_id=payload["producer_run_id"],
            source_count=payload["source_count"],
            metric=payload["metric"],
            threshold_stratum=payload["threshold_stratum"],
            configuration_identity=payload["configuration_identity"],
            data_identity=payload["data_identity"],
            flat_binding_id=payload["flat_binding_id"],
            hnsw_binding_id=payload["hnsw_binding_id"],
            environment_manifest_sha256=payload["environment_manifest_sha256"],
            execution_bound=bound,
            envelope_sha256=document["envelope_sha256"],
        )
        gate_c_bounded_execution_envelope_document_v3(envelope)
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error(code) from exc
    return envelope


def parse_gate_c_bounded_execution_envelope_document_v3(
    document: Mapping[str, object],
) -> GateCBoundedExecutionEnvelopeV3:
    return _envelope_v3_from_document(document)


def verify_gate_c_bounded_execution_envelope_v3(
    document: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    campaign_binding: Exp012ScaleCampaignBinding,
    source_head: VerifiedHostSourceHead,
    sources: tuple[CommittedHostObservation, ...],
    execution_source_revision: str,
) -> GateCBoundedExecutionEnvelopeV3:
    supplied = _envelope_v3_from_document(document)
    expected = build_gate_c_bounded_execution_envelope_v3(
        plan=plan,
        campaign_binding=campaign_binding,
        source_head=source_head,
        sources=sources,
        execution_bound=supplied.execution_bound,
        execution_source_revision=execution_source_revision,
        execution_environment_attestation=supplied.execution_environment_attestation,
    )
    if any(
        type(getattr(supplied, item.name)) is not type(getattr(expected, item.name))
        or getattr(supplied, item.name) != getattr(expected, item.name)
        for item in fields(GateCBoundedExecutionEnvelopeV3)
    ):
        raise _error("GATE_C_BOUNDED_ENVELOPE_V3_MISMATCH")
    return expected


def _count(value: object, *, code: str) -> int:
    if type(value) is not int or value < 0:
        raise _error(code)
    return value


def _governed_window_telemetry_count() -> int:
    counts: set[int] = set()
    for profile in Exp012ScaleProfile:
        contract = build_exp012_scale_contract(profile)
        counts.add(contract.window_query_count * contract.searches_per_source)
    if len(counts) != 1:
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    return counts.pop()


def _optional_head(value: object, *, count: int, code: str) -> str | None:
    if count == 0:
        if value is not None:
            raise _error(code)
        return None
    return _sha(value, code=code)


def build_gate_c_canonical_state(
    *,
    next_window_sequence: int,
    acknowledgement_count: int,
    acknowledgement_head_sha256: str | None,
    attempt_count: int,
    attempt_event_count: int,
    attempt_event_head_sha256: str | None,
    detector_event_count: int,
    detector_event_head_sha256: str | None,
    attestation_record_count: int,
    attestation_record_head_sha256: str | None,
    finalization_window_count: int,
    finalization_event_count: int,
    finalization_event_head_sha256: str | None,
    telemetry_record_count: int,
    telemetry_record_head_sha256: str | None,
) -> dict[str, object]:
    """Build one non-authorizing projection of verified canonical stores."""

    values = {
        "next_window_sequence": _count(
            next_window_sequence, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "acknowledgement_count": _count(
            acknowledgement_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "attempt_count": _count(
            attempt_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "attempt_event_count": _count(
            attempt_event_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "detector_event_count": _count(
            detector_event_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "attestation_record_count": _count(
            attestation_record_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "finalization_window_count": _count(
            finalization_window_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "finalization_event_count": _count(
            finalization_event_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
        "telemetry_record_count": _count(
            telemetry_record_count, code="GATE_C_CHECKPOINT_STATE_INVALID"
        ),
    }
    payload: dict[str, object] = {
        "schema_version": _STATE_SCHEMA_VERSION,
        **values,
        "acknowledgement_head_sha256": _optional_head(
            acknowledgement_head_sha256,
            count=values["acknowledgement_count"],
            code="GATE_C_CHECKPOINT_STATE_INVALID",
        ),
        "attempt_event_head_sha256": _optional_head(
            attempt_event_head_sha256,
            count=values["attempt_event_count"],
            code="GATE_C_CHECKPOINT_STATE_INVALID",
        ),
        "detector_event_head_sha256": _optional_head(
            detector_event_head_sha256,
            count=values["detector_event_count"],
            code="GATE_C_CHECKPOINT_STATE_INVALID",
        ),
        "attestation_record_head_sha256": _optional_head(
            attestation_record_head_sha256,
            count=values["attestation_record_count"],
            code="GATE_C_CHECKPOINT_STATE_INVALID",
        ),
        "finalization_event_head_sha256": _optional_head(
            finalization_event_head_sha256,
            count=values["finalization_event_count"],
            code="GATE_C_CHECKPOINT_STATE_INVALID",
        ),
        "telemetry_record_head_sha256": _optional_head(
            telemetry_record_head_sha256,
            count=values["telemetry_record_count"],
            code="GATE_C_CHECKPOINT_STATE_INVALID",
        ),
    }
    return {
        "state_payload": payload,
        "state_sha256": strict_canonical_digest(_STATE_DOMAIN, payload),
    }


def verify_gate_c_canonical_state(document: Mapping[str, object]) -> dict[str, object]:
    if type(document) is not dict or set(document) != {"state_payload", "state_sha256"}:
        raise _error("GATE_C_CHECKPOINT_STATE_INVALID")
    payload = document["state_payload"]
    expected = {
        "schema_version", "next_window_sequence", "acknowledgement_count",
        "acknowledgement_head_sha256", "attempt_count", "attempt_event_count",
        "attempt_event_head_sha256", "detector_event_count",
        "detector_event_head_sha256", "attestation_record_count",
        "attestation_record_head_sha256", "finalization_window_count",
        "finalization_event_count", "finalization_event_head_sha256",
        "telemetry_record_count", "telemetry_record_head_sha256",
    }
    if type(payload) is not dict or set(payload) != expected:
        raise _error("GATE_C_CHECKPOINT_STATE_INVALID")
    try:
        rebuilt = build_gate_c_canonical_state(
            **{name: payload[name] for name in expected if name != "schema_version"}
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_CHECKPOINT_STATE_INVALID") from exc
    if payload["schema_version"] != _STATE_SCHEMA_VERSION or document != rebuilt:
        raise _error("GATE_C_CHECKPOINT_STATE_INVALID")
    return rebuilt


def build_gate_c_window_checkpoint_effect(
    *,
    window_sequence: int,
    source_window_sha256: str,
    attempt_sha256s: tuple[str, ...],
    attempt_event_head_sha256: str,
    detector_event_sha256: str,
    detector_status: str,
    detector_head_sha256: str | None,
    attestation_disposition: str,
    attestation_record_sha256: str | None,
    attestation_record_head_sha256: str | None,
    attestation_sha256: str | None,
    prepared_sha256: str,
    acknowledgement_head_sha256: str,
    finalization_event_head_sha256: str,
    telemetry_record_count: int,
    telemetry_record_head_sha256: str,
) -> dict[str, object]:
    if (
        type(window_sequence) is not int
        or window_sequence < 0
        or type(attempt_sha256s) is not tuple
        or len(attempt_sha256s) != TRACE_COUNT
        or len(set(attempt_sha256s)) != TRACE_COUNT
    ):
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    attempts = [
        _sha(item, code="GATE_C_CHECKPOINT_EFFECT_INVALID")
        for item in attempt_sha256s
    ]
    status = _text(detector_status, code="GATE_C_CHECKPOINT_EFFECT_INVALID")
    disposition = _text(
        attestation_disposition, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
    )
    if status == "EVALUATED":
        if disposition != "COMMITTED":
            raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
        detector_head = _sha(
            detector_head_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        )
        attestation_record = _sha(
            attestation_record_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        )
        attestation_record_head = _sha(
            attestation_record_head_sha256,
            code="GATE_C_CHECKPOINT_EFFECT_INVALID",
        )
        attestation = _sha(
            attestation_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        )
    elif status == "REBASELINE":
        if (
            disposition != "NOT_REQUIRED"
            or detector_head_sha256 is not None
            or attestation_record_sha256 is not None
            or attestation_sha256 is not None
        ):
            raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
        detector_head = attestation_record = attestation = None
        attestation_record_head = (
            None
            if attestation_record_head_sha256 is None
            else _sha(
                attestation_record_head_sha256,
                code="GATE_C_CHECKPOINT_EFFECT_INVALID",
            )
        )
    else:
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    telemetry_count = _count(
        telemetry_record_count, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
    )
    if telemetry_count != _governed_window_telemetry_count():
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    payload: dict[str, object] = {
        "schema_version": _EFFECT_SCHEMA_VERSION,
        "window_sequence": window_sequence,
        "source_window_sha256": _sha(
            source_window_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        ),
        "attempt_sha256s": attempts,
        "attempt_event_head_sha256": _sha(
            attempt_event_head_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        ),
        "detector_event_sha256": _sha(
            detector_event_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        ),
        "detector_status": status,
        "detector_head_sha256": detector_head,
        "attestation_disposition": disposition,
        "attestation_record_sha256": attestation_record,
        "attestation_record_head_sha256": attestation_record_head,
        "attestation_sha256": attestation,
        "prepared_sha256": _sha(
            prepared_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        ),
        "acknowledgement_head_sha256": _sha(
            acknowledgement_head_sha256, code="GATE_C_CHECKPOINT_EFFECT_INVALID"
        ),
        "finalization_event_head_sha256": _sha(
            finalization_event_head_sha256,
            code="GATE_C_CHECKPOINT_EFFECT_INVALID",
        ),
        "telemetry_record_count": telemetry_count,
        "telemetry_record_head_sha256": _sha(
            telemetry_record_head_sha256,
            code="GATE_C_CHECKPOINT_EFFECT_INVALID",
        ),
    }
    return {
        "effect_payload": payload,
        "effect_sha256": strict_canonical_digest(_EFFECT_DOMAIN, payload),
    }


def verify_gate_c_window_checkpoint_effect(
    document: Mapping[str, object],
) -> dict[str, object]:
    if type(document) is not dict or set(document) != {"effect_payload", "effect_sha256"}:
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    payload = document["effect_payload"]
    fields = {
        "window_sequence", "source_window_sha256", "attempt_sha256s",
        "attempt_event_head_sha256", "detector_event_sha256", "detector_status",
        "detector_head_sha256", "attestation_disposition",
        "attestation_record_sha256", "attestation_record_head_sha256",
        "attestation_sha256", "prepared_sha256",
        "acknowledgement_head_sha256", "finalization_event_head_sha256",
        "telemetry_record_count", "telemetry_record_head_sha256",
    }
    if type(payload) is not dict or set(payload) != {"schema_version", *fields}:
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    try:
        rebuilt = build_gate_c_window_checkpoint_effect(
            **{
                name: (
                    tuple(payload[name])
                    if name == "attempt_sha256s"
                    else payload[name]
                )
                for name in fields
            }
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID") from exc
    if payload["schema_version"] != _EFFECT_SCHEMA_VERSION or document != rebuilt:
        raise _error("GATE_C_CHECKPOINT_EFFECT_INVALID")
    return rebuilt


def build_gate_c_checkpoint_result(
    *,
    envelope: GateCBoundedExecutionEnvelope,
    pre_state: Mapping[str, object],
    post_state: Mapping[str, object],
    processed_window_sequences: tuple[int, ...],
    checkpoint_effects: tuple[Mapping[str, object], ...],
) -> dict[str, object]:
    gate_c_bounded_execution_envelope_document(envelope)
    bound = envelope.execution_bound
    pre = verify_gate_c_canonical_state(pre_state)
    post = verify_gate_c_canonical_state(post_state)
    effects = tuple(
        verify_gate_c_window_checkpoint_effect(item) for item in checkpoint_effects
    )
    pre_payload = pre["state_payload"]
    post_payload = post["state_payload"]
    if (
        type(processed_window_sequences) is not tuple
        or any(type(item) is not int for item in processed_window_sequences)
        or processed_window_sequences != bound.allowed_window_sequences
        or len(effects) != bound.window_count
        or tuple(item["effect_payload"]["window_sequence"] for item in effects)
        != bound.allowed_window_sequences
        or pre_payload["next_window_sequence"] != bound.start_window_sequence
        or post_payload["next_window_sequence"]
        != bound.expected_next_window_sequence
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    expected = {
        "acknowledgement_count": WINDOW_QUERY_COUNT,
        "attempt_count": TRACE_COUNT,
        "attempt_event_count": TRACE_COUNT * 2,
        "detector_event_count": 1,
        "finalization_window_count": 1,
        "finalization_event_count": 5,
        "telemetry_record_count": (
            WINDOW_QUERY_COUNT * envelope.scale_contract.searches_per_source
        ),
    }
    count_fields = tuple(expected)
    for name in count_fields:
        if (
            pre_payload[name] != bound.start_window_sequence * expected[name]
            or post_payload[name]
            != bound.expected_next_window_sequence * expected[name]
        ):
            raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    expected_effect_telemetry = expected["telemetry_record_count"]
    effect_telemetry_counts = tuple(
        item["effect_payload"]["telemetry_record_count"] for item in effects
    )
    if (
        any(item != expected_effect_telemetry for item in effect_telemetry_counts)
        or sum(effect_telemetry_counts)
        != post_payload["telemetry_record_count"]
        - pre_payload["telemetry_record_count"]
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    committed = sum(
        item["effect_payload"]["attestation_disposition"] == "COMMITTED"
        for item in effects
    )
    if (
        post_payload["attestation_record_count"]
        - pre_payload["attestation_record_count"]
        != committed
        or post_payload["acknowledgement_head_sha256"]
        != effects[-1]["effect_payload"]["acknowledgement_head_sha256"]
        or post_payload["attempt_event_head_sha256"]
        != effects[-1]["effect_payload"]["attempt_event_head_sha256"]
        or post_payload["detector_event_head_sha256"]
        != effects[-1]["effect_payload"]["detector_event_sha256"]
        or post_payload["finalization_event_head_sha256"]
        != effects[-1]["effect_payload"]["finalization_event_head_sha256"]
        or post_payload["telemetry_record_head_sha256"]
        != effects[-1]["effect_payload"]["telemetry_record_head_sha256"]
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    if (
        post_payload["attestation_record_head_sha256"]
        != effects[-1]["effect_payload"]["attestation_record_head_sha256"]
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    counts = {
        name.removesuffix("_count"): {
            "pre": pre_payload[name],
            "post": post_payload[name],
            "delta": post_payload[name] - pre_payload[name],
        }
        for name in (
            "acknowledgement_count", "attempt_count", "telemetry_record_count"
        )
    }
    payload: dict[str, object] = {
        "schema_version": CHECKPOINT_RESULT_SCHEMA_VERSION,
        "experiment_id": "EXP-012-SCALE",
        "envelope_sha256": envelope.envelope_sha256,
        "pre_state": pre,
        "post_state": post,
        "processed_window_sequences": list(processed_window_sequences),
        "checkpoint_effects": list(effects),
        "checkpoint_counts": counts,
        "full_campaign_complete": False,
    }
    return {
        "checkpoint_result_payload": payload,
        "checkpoint_result_sha256": strict_canonical_digest(_RESULT_DOMAIN, payload),
    }


def verify_gate_c_checkpoint_result(
    document: Mapping[str, object], *, envelope: GateCBoundedExecutionEnvelope
) -> dict[str, object]:
    if type(document) is not dict or set(document) != {
        "checkpoint_result_payload", "checkpoint_result_sha256"
    }:
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    payload = document["checkpoint_result_payload"]
    if type(payload) is not dict or set(payload) != {
        "schema_version", "experiment_id", "envelope_sha256",
        "pre_state", "post_state", "processed_window_sequences",
        "checkpoint_effects", "checkpoint_counts", "full_campaign_complete",
    }:
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    try:
        rebuilt = build_gate_c_checkpoint_result(
            envelope=envelope,
            pre_state=payload["pre_state"],
            post_state=payload["post_state"],
            processed_window_sequences=tuple(payload["processed_window_sequences"]),
            checkpoint_effects=tuple(payload["checkpoint_effects"]),
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID") from exc
    if document != rebuilt:
        raise _error("GATE_C_CHECKPOINT_RESULT_INVALID")
    return rebuilt


def build_gate_c_checkpoint_result_v2(
    *,
    envelope: GateCBoundedExecutionEnvelopeV2,
    pre_state: Mapping[str, object],
    post_state: Mapping[str, object],
    processed_window_sequences: tuple[int, ...],
    checkpoint_effects: tuple[Mapping[str, object], ...],
) -> dict[str, object]:
    gate_c_bounded_execution_envelope_document_v2(envelope)
    bound = envelope.execution_bound
    pre = verify_gate_c_canonical_state(pre_state)
    post = verify_gate_c_canonical_state(post_state)
    effects = tuple(
        verify_gate_c_window_checkpoint_effect(item) for item in checkpoint_effects
    )
    pre_payload = pre["state_payload"]
    post_payload = post["state_payload"]
    if (
        type(processed_window_sequences) is not tuple
        or any(type(item) is not int for item in processed_window_sequences)
        or processed_window_sequences != bound.allowed_window_sequences
        or len(effects) != bound.window_count
        or tuple(item["effect_payload"]["window_sequence"] for item in effects)
        != bound.allowed_window_sequences
        or pre_payload["next_window_sequence"] != bound.start_window_sequence
        or post_payload["next_window_sequence"]
        != bound.expected_next_window_sequence
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    expected = {
        "acknowledgement_count": WINDOW_QUERY_COUNT,
        "attempt_count": TRACE_COUNT,
        "attempt_event_count": TRACE_COUNT * 2,
        "detector_event_count": 1,
        "finalization_window_count": 1,
        "finalization_event_count": 5,
        "telemetry_record_count": (
            WINDOW_QUERY_COUNT * envelope.scale_contract.searches_per_source
        ),
    }
    for name, per_window in expected.items():
        if (
            pre_payload[name] != bound.start_window_sequence * per_window
            or post_payload[name] != bound.expected_next_window_sequence * per_window
        ):
            raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    expected_effect_telemetry = expected["telemetry_record_count"]
    effect_telemetry_counts = tuple(
        item["effect_payload"]["telemetry_record_count"] for item in effects
    )
    if (
        any(item != expected_effect_telemetry for item in effect_telemetry_counts)
        or sum(effect_telemetry_counts)
        != post_payload["telemetry_record_count"]
        - pre_payload["telemetry_record_count"]
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    committed = sum(
        item["effect_payload"]["attestation_disposition"] == "COMMITTED"
        for item in effects
    )
    if (
        post_payload["attestation_record_count"]
        - pre_payload["attestation_record_count"]
        != committed
        or post_payload["acknowledgement_head_sha256"]
        != effects[-1]["effect_payload"]["acknowledgement_head_sha256"]
        or post_payload["attempt_event_head_sha256"]
        != effects[-1]["effect_payload"]["attempt_event_head_sha256"]
        or post_payload["detector_event_head_sha256"]
        != effects[-1]["effect_payload"]["detector_event_sha256"]
        or post_payload["finalization_event_head_sha256"]
        != effects[-1]["effect_payload"]["finalization_event_head_sha256"]
        or post_payload["telemetry_record_head_sha256"]
        != effects[-1]["effect_payload"]["telemetry_record_head_sha256"]
        or post_payload["attestation_record_head_sha256"]
        != effects[-1]["effect_payload"]["attestation_record_head_sha256"]
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    counts = {
        name.removesuffix("_count"): {
            "pre": pre_payload[name],
            "post": post_payload[name],
            "delta": post_payload[name] - pre_payload[name],
        }
        for name in (
            "acknowledgement_count", "attempt_count", "telemetry_record_count"
        )
    }
    payload: dict[str, object] = {
        "schema_version": CHECKPOINT_RESULT_SCHEMA_VERSION_V2,
        "experiment_id": "EXP-012-SCALE",
        "envelope_sha256": envelope.envelope_sha256,
        "source_revision": envelope.source_revision,
        "execution_source_revision": envelope.execution_source_revision,
        "pre_state": pre,
        "post_state": post,
        "processed_window_sequences": list(processed_window_sequences),
        "checkpoint_effects": list(effects),
        "checkpoint_counts": counts,
        "full_campaign_complete": False,
    }
    return {
        "checkpoint_result_payload": payload,
        "checkpoint_result_sha256": strict_canonical_digest(
            _RESULT_DOMAIN_V2, payload
        ),
    }


def verify_gate_c_checkpoint_result_v2(
    document: Mapping[str, object], *, envelope: GateCBoundedExecutionEnvelopeV2
) -> dict[str, object]:
    if type(document) is not dict or set(document) != {
        "checkpoint_result_payload", "checkpoint_result_sha256"
    }:
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    payload = document["checkpoint_result_payload"]
    if type(payload) is not dict or set(payload) != {
        "schema_version", "experiment_id", "envelope_sha256",
        "source_revision", "execution_source_revision", "pre_state",
        "post_state", "processed_window_sequences", "checkpoint_effects",
        "checkpoint_counts", "full_campaign_complete",
    }:
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    try:
        rebuilt = build_gate_c_checkpoint_result_v2(
            envelope=envelope,
            pre_state=payload["pre_state"],
            post_state=payload["post_state"],
            processed_window_sequences=tuple(payload["processed_window_sequences"]),
            checkpoint_effects=tuple(payload["checkpoint_effects"]),
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID") from exc
    if document != rebuilt:
        raise _error("GATE_C_CHECKPOINT_RESULT_V2_INVALID")
    return rebuilt


def build_gate_c_checkpoint_result_v3(
    *,
    envelope: GateCBoundedExecutionEnvelopeV3,
    pre_state: Mapping[str, object],
    post_state: Mapping[str, object],
    processed_window_sequences: tuple[int, ...],
    checkpoint_effects: tuple[Mapping[str, object], ...],
) -> dict[str, object]:
    gate_c_bounded_execution_envelope_document_v3(envelope)
    bound = envelope.execution_bound
    pre = verify_gate_c_canonical_state(pre_state)
    post = verify_gate_c_canonical_state(post_state)
    effects = tuple(
        verify_gate_c_window_checkpoint_effect(item) for item in checkpoint_effects
    )
    pre_payload = pre["state_payload"]
    post_payload = post["state_payload"]
    if (
        type(processed_window_sequences) is not tuple
        or any(type(item) is not int for item in processed_window_sequences)
        or processed_window_sequences != bound.allowed_window_sequences
        or len(effects) != bound.window_count
        or tuple(item["effect_payload"]["window_sequence"] for item in effects)
        != bound.allowed_window_sequences
        or pre_payload["next_window_sequence"] != bound.start_window_sequence
        or post_payload["next_window_sequence"] != bound.expected_next_window_sequence
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    expected = {
        "acknowledgement_count": WINDOW_QUERY_COUNT,
        "attempt_count": TRACE_COUNT,
        "attempt_event_count": TRACE_COUNT * 2,
        "detector_event_count": 1,
        "finalization_window_count": 1,
        "finalization_event_count": 5,
        "telemetry_record_count": (
            WINDOW_QUERY_COUNT * envelope.scale_contract.searches_per_source
        ),
    }
    for name, per_window in expected.items():
        if (
            pre_payload[name] != bound.start_window_sequence * per_window
            or post_payload[name] != bound.expected_next_window_sequence * per_window
        ):
            raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    if any(
        effect["effect_payload"]["telemetry_record_count"]
        != expected["telemetry_record_count"]
        for effect in effects
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    if sum(
        effect["effect_payload"]["telemetry_record_count"] for effect in effects
    ) != post_payload["telemetry_record_count"] - pre_payload["telemetry_record_count"]:
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    committed = sum(
        effect["effect_payload"]["attestation_disposition"] == "COMMITTED"
        for effect in effects
    )
    final_effect = effects[-1]["effect_payload"]
    if (
        post_payload["attestation_record_count"]
        - pre_payload["attestation_record_count"]
        != committed
        or post_payload["acknowledgement_head_sha256"]
        != final_effect["acknowledgement_head_sha256"]
        or post_payload["attempt_event_head_sha256"]
        != final_effect["attempt_event_head_sha256"]
        or post_payload["detector_event_head_sha256"]
        != final_effect["detector_event_sha256"]
        or post_payload["attestation_record_head_sha256"]
        != final_effect["attestation_record_head_sha256"]
        or post_payload["finalization_event_head_sha256"]
        != final_effect["finalization_event_head_sha256"]
        or post_payload["telemetry_record_head_sha256"]
        != final_effect["telemetry_record_head_sha256"]
    ):
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    counts = {
        name.removesuffix("_count"): {
            "pre": pre_payload[name],
            "post": post_payload[name],
            "delta": post_payload[name] - pre_payload[name],
        }
        for name in (
            "acknowledgement_count", "attempt_count", "telemetry_record_count"
        )
    }
    payload: dict[str, object] = {
        "schema_version": CHECKPOINT_RESULT_SCHEMA_VERSION_V3,
        "experiment_id": "EXP-012-SCALE",
        "envelope_sha256": envelope.envelope_sha256,
        "source_revision": envelope.source_revision,
        "execution_source_revision": envelope.execution_source_revision,
        "execution_environment_identity_sha256": (
            envelope.execution_environment_identity_sha256
        ),
        "execution_environment_attestation_sha256": (
            envelope.execution_environment_attestation_sha256
        ),
        "pre_state": pre,
        "post_state": post,
        "processed_window_sequences": list(processed_window_sequences),
        "checkpoint_effects": list(effects),
        "checkpoint_counts": counts,
        "full_campaign_complete": False,
    }
    return {
        "checkpoint_result_payload": payload,
        "checkpoint_result_sha256": strict_canonical_digest(
            _RESULT_DOMAIN_V3, payload
        ),
    }


def verify_gate_c_checkpoint_result_v3(
    document: Mapping[str, object], *, envelope: GateCBoundedExecutionEnvelopeV3
) -> dict[str, object]:
    if type(document) is not dict or set(document) != {
        "checkpoint_result_payload", "checkpoint_result_sha256"
    }:
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    payload = document["checkpoint_result_payload"]
    if type(payload) is not dict or set(payload) != {
        "schema_version", "experiment_id", "envelope_sha256", "source_revision",
        "execution_source_revision", "execution_environment_identity_sha256",
        "execution_environment_attestation_sha256", "pre_state", "post_state",
        "processed_window_sequences", "checkpoint_effects", "checkpoint_counts",
        "full_campaign_complete",
    }:
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    try:
        rebuilt = build_gate_c_checkpoint_result_v3(
            envelope=envelope,
            pre_state=payload["pre_state"],
            post_state=payload["post_state"],
            processed_window_sequences=tuple(payload["processed_window_sequences"]),
            checkpoint_effects=tuple(payload["checkpoint_effects"]),
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, GateCBoundedExecutionError):
            raise
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID") from exc
    if document != rebuilt:
        raise _error("GATE_C_CHECKPOINT_RESULT_V3_INVALID")
    return rebuilt
