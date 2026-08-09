"""Restart-durable file adapters for the ADR-002 actuation boundary.

Purpose:
    Persist immutable actuation audit records and the automatic-action disable
    switch without adding Milvus, detector, or policy logic.
Inputs:
    Frozen ``ActuationAuditRecord`` values, externally supplied audit identity,
    and an injectable RFC3339-UTC clock.
Outputs:
    An append-only, process-locked JSONL audit and an atomic controller state.
Dependencies:
    Python's POSIX file-locking and filesystem primitives only; never PyMilvus.
Failure modes:
    Duplicate audit IDs and malformed audit logs raise. Missing controller state
    is enabled; unreadable or malformed controller state is disabled fail-closed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, TypeAlias
import unicodedata

from .actuation import (
    ActuationAuditRecord,
    ActuationIdentityContext,
    ActuationOutcome,
    RollbackActuationContext,
    RollbackVerification,
)
from .config import Metric, THRESHOLD_LABELS
from .drift import EvidenceProvenance, evidence_provenance_valid
from .policy import ACTUATION_LADDER, PolicyAction, SafetyGateResult

HISTORICAL_AUDIT_SCHEMA_VERSION = 2
AUDIT_SCHEMA_VERSION = 3
ACTUATION_CONTEXT_SCHEMA_VERSION = "actuation-context-v3"
CONTROLLER_SCHEMA_VERSION = 1
REENABLE_CONFIRMATION_TOKEN = "I_CONFIRM_RE_ENABLE_AUTOMATIC_ACTIONS"
AUDIT_QUERY_COUNT = 50

Clock: TypeAlias = Callable[[], str]

_AUDIT_ENVELOPE_FIELDS = frozenset({"schema_version", "record"})
_AUDIT_RECORD_FIELDS = frozenset(
    {
        "audit_id",
        "action",
        "outcome",
        "attempted",
        "success",
        "reason",
        "context",
        "current_ef",
        "candidate_ef",
        "last_known_good_ef",
        "traffic_fraction",
        "policy_reason",
        "safety_gate_results",
        "shadow_result",
        "canary_observation",
        "rollback_verification",
        "automatic_actions_disabled",
        "evidence_provenance",
    }
)
_HISTORICAL_V2_CONTEXT_FIELDS = frozenset(
    {
        "metric",
        "threshold_stratum",
        "collection_name",
        "configuration_identity",
        "index_identity",
        "flat_index_identity",
        "data_identity",
        "audited_query_ids",
        "last_known_good",
        "occurred_at_utc",
    }
)
_HISTORICAL_V2_QUALIFICATION_FIELDS = frozenset(
    {
        "qualified",
        "ef",
        "reasons",
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "index_identity",
        "data_identity",
        "qualifying_window_ids",
    }
)
_V3_COMMON_CONTEXT_FIELDS = frozenset(
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
    }
)
_V3_ROLLBACK_CONTEXT_FIELDS = frozenset(
    {*_V3_COMMON_CONTEXT_FIELDS, "expected_last_known_good_ef", "audited_query_ids"}
)
_SAFETY_GATE_FIELDS = frozenset({"name", "passed", "detail"})
_HISTORICAL_V2_SHADOW_FIELDS = frozenset(
    {
        "success",
        "audited_query_count",
        "failed_query_count",
        "timeout_query_count",
        "threshold_violation_count",
        "candidate_flat_oracle_agreement",
        "last_known_good_flat_oracle_agreement",
        "detail",
    }
)
_HISTORICAL_V2_CANARY_FIELDS = frozenset(
    {
        "metric",
        "threshold_stratum",
        "candidate_ef",
        "last_known_good_ef",
        "completed_query_count",
        "candidate_mean_recall",
        "candidate_recall_lower_bound_95",
        "last_known_good_mean_recall",
        "candidate_p95_latency_ms",
        "candidate_latency_upper_bound_95_ms",
        "last_known_good_p95_latency_ms",
        "configuration_identity",
        "index_identity",
        "data_identity",
        "failed_query_count",
        "timeout_query_count",
        "threshold_violation_count",
        "flat_oracle_agreement",
        "milvus_healthy",
        "etcd_healthy",
        "minio_healthy",
        "collection_loaded",
        "configuration_valid",
        "index_identity_unchanged",
        "audit_record_present",
        "actuation_exception",
    }
)
_ROLLBACK_VERIFICATION_FIELDS = frozenset(
    {
        "success",
        "restored_ef",
        "health_passed",
        "audit_passed",
        "configuration_identity",
        "index_identity",
        "data_identity",
        "detail",
    }
)
_EVIDENCE_PROVENANCE_FIELDS = frozenset(
    {
        "schema_version",
        "metric",
        "threshold_stratum",
        "reference_window_id",
        "current_window_id",
        "reference_manifest_sha256",
        "current_manifest_sha256",
        "configuration_identity",
        "data_identity",
        "flat_binding_id",
        "hnsw_binding_id",
        "reference_audit_ids",
        "reference_audit_rank_digests",
        "current_audit_ids",
        "current_audit_rank_digests",
        "sha256",
    }
)
_CONTROLLER_FIELDS = frozenset(
    {
        "schema_version",
        "state",
        "audit_id",
        "reason",
        "changed_at_utc",
        "confirmed_by",
    }
)
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class AuditLogCorruptedError(RuntimeError):
    """Raised when any historical-v2 or current-v3 audit line is untrustworthy."""


class DuplicateAuditIdError(ValueError):
    """Raised when append would reuse an immutable audit identity."""


class _DuplicateJsonField(ValueError):
    """Internal marker for duplicate JSON object keys."""


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_rfc3339_utc(value: object) -> bool:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    offset = parsed.utcoffset()
    return offset is not None and offset.total_seconds() == 0


def _current_rfc3339_utc() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _object_without_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _exact_mapping(value: object, expected_fields: frozenset[str]) -> bool:
    return isinstance(value, Mapping) and frozenset(value) == expected_fields


def _valid_optional_mapping(
    value: object,
    expected_fields: frozenset[str],
) -> bool:
    return value is None or _exact_mapping(value, expected_fields)


def _valid_ef(value: object) -> bool:
    return type(value) is int and value in ACTUATION_LADDER


def _valid_optional_ef(value: object) -> bool:
    return value is None or _valid_ef(value)


def _valid_observed_ef(value: object) -> bool:
    """Accept an exact integer observation without granting actuation authority."""

    return type(value) is int


def _valid_optional_observed_ef(value: object) -> bool:
    return value is None or _valid_observed_ef(value)


def _valid_query_ids(value: object) -> bool:
    if not isinstance(value, (list, tuple)) or len(value) != AUDIT_QUERY_COUNT:
        return False
    seen: set[tuple[type, int | str]] = set()
    for query_id in value:
        if type(query_id) not in {int, str}:
            return False
        if isinstance(query_id, str):
            normalized = unicodedata.normalize("NFC", query_id)
            if not normalized or normalized != query_id:
                return False
        key = (type(query_id), query_id)
        if key in seen:
            return False
        seen.add(key)
    return True


def _validate_safety_gates(value: object) -> None:
    if not isinstance(value, list):
        raise AuditLogCorruptedError("audit safety gates must be a list")
    for gate in value:
        if not _exact_mapping(gate, _SAFETY_GATE_FIELDS):
            raise AuditLogCorruptedError(
                "audit safety-gate fields do not match schema"
            )
        assert isinstance(gate, Mapping)
        if (
            not _nonempty(gate["name"])
            or type(gate["passed"]) is not bool
            or not isinstance(gate["detail"], str)
        ):
            raise AuditLogCorruptedError("audit safety gate is malformed")


def _validate_evidence_provenance(value: object) -> None:
    if value is None:
        return
    if not _exact_mapping(value, _EVIDENCE_PROVENANCE_FIELDS):
        raise AuditLogCorruptedError(
            "audit evidence provenance fields do not match schema"
        )
    assert isinstance(value, Mapping)
    try:
        provenance = EvidenceProvenance(
            schema_version=value["schema_version"],
            metric=Metric(value["metric"]),
            threshold_stratum=value["threshold_stratum"],
            reference_window_id=value["reference_window_id"],
            current_window_id=value["current_window_id"],
            reference_manifest_sha256=value["reference_manifest_sha256"],
            current_manifest_sha256=value["current_manifest_sha256"],
            configuration_identity=value["configuration_identity"],
            data_identity=value["data_identity"],
            flat_binding_id=value["flat_binding_id"],
            hnsw_binding_id=value["hnsw_binding_id"],
            reference_audit_ids=tuple(value["reference_audit_ids"]),
            reference_audit_rank_digests=tuple(value["reference_audit_rank_digests"]),
            current_audit_ids=tuple(value["current_audit_ids"]),
            current_audit_rank_digests=tuple(value["current_audit_rank_digests"]),
            sha256=value["sha256"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AuditLogCorruptedError("audit evidence provenance is malformed") from exc
    if not evidence_provenance_valid(provenance):
        raise AuditLogCorruptedError("audit evidence provenance is invalid")


def _validate_historical_v2_record(record: object) -> str:
    if not _exact_mapping(record, _AUDIT_RECORD_FIELDS):
        raise AuditLogCorruptedError("historical v2 record fields do not match schema")
    assert isinstance(record, Mapping)
    if not _nonempty(record["audit_id"]):
        raise AuditLogCorruptedError("historical v2 audit_id is malformed")
    if not _exact_mapping(record["context"], _HISTORICAL_V2_CONTEXT_FIELDS):
        raise AuditLogCorruptedError("historical v2 context fields do not match schema")
    context = record["context"]
    assert isinstance(context, Mapping)
    if not _exact_mapping(
        context["last_known_good"], _HISTORICAL_V2_QUALIFICATION_FIELDS
    ):
        raise AuditLogCorruptedError(
            "historical v2 last-known-good fields do not match schema"
        )
    gates = record["safety_gate_results"]
    if not isinstance(gates, list) or any(
        not _exact_mapping(gate, _SAFETY_GATE_FIELDS) for gate in gates
    ):
        raise AuditLogCorruptedError(
            "historical v2 safety-gate fields do not match schema"
        )
    optional_fields = (
        (
            record["shadow_result"],
            _HISTORICAL_V2_SHADOW_FIELDS,
            "shadow result",
        ),
        (
            record["canary_observation"],
            _HISTORICAL_V2_CANARY_FIELDS,
            "canary observation",
        ),
        (
            record["rollback_verification"],
            _ROLLBACK_VERIFICATION_FIELDS,
            "rollback verification",
        ),
    )
    for value, expected_fields, label in optional_fields:
        if not _valid_optional_mapping(value, expected_fields):
            raise AuditLogCorruptedError(f"audit {label} fields do not match schema")
    _validate_evidence_provenance(record["evidence_provenance"])
    return record["audit_id"]


def _validate_v3_common_context(context: Mapping[str, Any]) -> None:
    if context["context_schema_version"] != ACTUATION_CONTEXT_SCHEMA_VERSION:
        raise AuditLogCorruptedError("unsupported actuation context schema")
    try:
        Metric(context["metric"])
    except (TypeError, ValueError) as exc:
        raise AuditLogCorruptedError("v3 context metric is invalid") from exc
    if (
        not isinstance(context["threshold_stratum"], str)
        or context["threshold_stratum"] not in THRESHOLD_LABELS
    ):
        raise AuditLogCorruptedError("v3 context threshold stratum is invalid")
    for field_name in (
        "collection_name",
        "configuration_identity",
        "index_identity",
        "flat_index_identity",
        "data_identity",
    ):
        if not _nonempty(context[field_name]):
            raise AuditLogCorruptedError(f"v3 context {field_name} is invalid")
    if not _valid_rfc3339_utc(context["occurred_at_utc"]):
        raise AuditLogCorruptedError("v3 context timestamp is invalid")


def _validate_v3_context(
    value: object,
    *,
    expected_kind: str,
    last_known_good_ef: object,
) -> Mapping[str, Any]:
    expected_fields = (
        _V3_COMMON_CONTEXT_FIELDS
        if expected_kind == "POLICY"
        else _V3_ROLLBACK_CONTEXT_FIELDS
    )
    if not _exact_mapping(value, expected_fields):
        raise AuditLogCorruptedError("v3 context fields do not match schema")
    assert isinstance(value, Mapping)
    if value["context_kind"] != expected_kind:
        raise AuditLogCorruptedError("v3 context kind does not match action")
    _validate_v3_common_context(value)
    if expected_kind == "ROLLBACK":
        expected_ef = value["expected_last_known_good_ef"]
        if not _valid_ef(expected_ef):
            raise AuditLogCorruptedError("v3 rollback context ef is invalid")
        if expected_ef != last_known_good_ef:
            raise AuditLogCorruptedError("v3 rollback context ef mismatches record")
        if not _valid_query_ids(value["audited_query_ids"]):
            raise AuditLogCorruptedError("v3 rollback audit query IDs are invalid")
    return value


def _validate_rollback_verification(value: object) -> Mapping[str, Any]:
    if not _exact_mapping(value, _ROLLBACK_VERIFICATION_FIELDS):
        raise AuditLogCorruptedError(
            "audit rollback-verification fields do not match schema"
        )
    assert isinstance(value, Mapping)
    if (
        type(value["success"]) is not bool
        or not _valid_optional_observed_ef(value["restored_ef"])
        or type(value["health_passed"]) is not bool
        or type(value["audit_passed"]) is not bool
        or not _nonempty(value["configuration_identity"])
        or not _nonempty(value["index_identity"])
        or not _nonempty(value["data_identity"])
        or not isinstance(value["detail"], str)
    ):
        raise AuditLogCorruptedError("audit rollback verification is malformed")
    if value["success"] is True and not (
        _valid_ef(value["restored_ef"])
        and value["health_passed"] is True
        and value["audit_passed"] is True
    ):
        raise AuditLogCorruptedError(
            "successful rollback verification is internally inconsistent"
        )
    return value


def _restoration_success_proven(
    verification: Mapping[str, Any] | None,
    context: Mapping[str, Any],
) -> bool:
    """Return whether verification proves the complete restoration contract."""

    return bool(
        verification is not None
        and verification["success"] is True
        and verification["restored_ef"]
        == context["expected_last_known_good_ef"]
        and verification["health_passed"] is True
        and verification["audit_passed"] is True
        and verification["configuration_identity"]
        == context["configuration_identity"]
        and verification["index_identity"] == context["index_identity"]
        and verification["data_identity"] == context["data_identity"]
    )


def _validate_v3_record(record: object) -> str:
    if not _exact_mapping(record, _AUDIT_RECORD_FIELDS):
        raise AuditLogCorruptedError("v3 audit record fields do not match schema")
    assert isinstance(record, Mapping)
    if not _nonempty(record["audit_id"]):
        raise AuditLogCorruptedError("v3 audit_id is malformed")
    try:
        action = PolicyAction(record["action"])
        outcome = ActuationOutcome(record["outcome"])
    except (TypeError, ValueError) as exc:
        raise AuditLogCorruptedError("v3 action or outcome is invalid") from exc
    if (
        type(record["attempted"]) is not bool
        or type(record["success"]) is not bool
        or type(record["automatic_actions_disabled"]) is not bool
        or not _nonempty(record["reason"])
        or not _nonempty(record["policy_reason"])
        or not _valid_observed_ef(record["current_ef"])
        or not _valid_optional_observed_ef(record["candidate_ef"])
        or not _valid_optional_observed_ef(record["last_known_good_ef"])
        or record["traffic_fraction"] is not None
        or record["shadow_result"] is not None
        or record["canary_observation"] is not None
    ):
        raise AuditLogCorruptedError("v3 audit record scalar invariant failed")
    _validate_safety_gates(record["safety_gate_results"])
    _validate_evidence_provenance(record["evidence_provenance"])

    verification = record["rollback_verification"]
    if verification is not None:
        verification = _validate_rollback_verification(verification)

    if action in {PolicyAction.NO_CHANGE, PolicyAction.RECOMMEND_EF}:
        _validate_v3_context(
            record["context"],
            expected_kind="POLICY",
            last_known_good_ef=record["last_known_good_ef"],
        )
        if not (
            outcome is ActuationOutcome.NO_OP
            and record["attempted"] is False
            and record["success"] is True
            and record["reason"] == "NON_ACTIONABLE_POLICY_DECISION"
            and verification is None
            and record["automatic_actions_disabled"] is False
        ):
            raise AuditLogCorruptedError("v3 non-actionable policy invariant failed")
    elif action is PolicyAction.START_CANARY:
        _validate_v3_context(
            record["context"],
            expected_kind="POLICY",
            last_known_good_ef=record["last_known_good_ef"],
        )
        if not (
            outcome is ActuationOutcome.BLOCKED
            and record["attempted"] is False
            and record["success"] is False
            and record["reason"] == "GENERIC_START_CANARY_RETIRED"
            and verification is None
            and record["automatic_actions_disabled"] is False
        ):
            raise AuditLogCorruptedError("v3 retired-start invariant failed")
    elif action is PolicyAction.ROLLBACK:
        context = _validate_v3_context(
            record["context"],
            expected_kind="ROLLBACK",
            last_known_good_ef=record["last_known_good_ef"],
        )
        restoration_success = _restoration_success_proven(verification, context)
        if outcome is ActuationOutcome.BLOCKED:
            valid = bool(
                record["attempted"] is False
                and record["success"] is False
                and verification is None
                and record["automatic_actions_disabled"] is False
                and record["reason"] != "ROLLBACK_VERIFIED"
            )
        elif outcome is ActuationOutcome.FAILED:
            valid = bool(
                record["attempted"] is True
                and record["success"] is False
                and record["automatic_actions_disabled"] is True
                and not restoration_success
                and record["reason"] != "ROLLBACK_VERIFIED"
            )
        elif outcome is ActuationOutcome.SUCCEEDED:
            valid = bool(
                record["attempted"] is True
                and record["success"] is True
                and record["reason"] == "ROLLBACK_VERIFIED"
                and restoration_success
                and record["automatic_actions_disabled"] is False
            )
        else:
            valid = False
        if not valid:
            raise AuditLogCorruptedError("v3 rollback outcome invariant failed")
    else:  # pragma: no cover - PolicyAction is closed above
        raise AuditLogCorruptedError("v3 action is unsupported")
    return record["audit_id"]


def _validate_audit_payload(payload: object) -> str:
    if not _exact_mapping(payload, _AUDIT_ENVELOPE_FIELDS):
        raise AuditLogCorruptedError("audit envelope fields do not match schema")
    assert isinstance(payload, Mapping)
    schema_version = payload["schema_version"]
    if type(schema_version) is not int:
        raise AuditLogCorruptedError("audit schema version must be an integer")
    if schema_version == HISTORICAL_AUDIT_SCHEMA_VERSION:
        return _validate_historical_v2_record(payload["record"])
    if schema_version == AUDIT_SCHEMA_VERSION:
        return _validate_v3_record(payload["record"])
    raise AuditLogCorruptedError("unsupported audit schema version")


def _decode_audit_line(line: str, *, line_number: int) -> str:
    try:
        payload = json.loads(line, object_pairs_hook=_object_without_duplicate_keys)
    except (json.JSONDecodeError, _DuplicateJsonField, TypeError, ValueError) as exc:
        raise AuditLogCorruptedError(
            f"malformed audit JSON at line {line_number}"
        ) from exc
    return _validate_audit_payload(payload)


def _scan_audit_handle(handle: Any) -> set[str]:
    handle.seek(0)
    try:
        payload = handle.read()
    except (OSError, UnicodeError) as exc:
        raise AuditLogCorruptedError("audit log is unreadable") from exc
    if payload and not payload.endswith("\n"):
        raise AuditLogCorruptedError("audit log has an incomplete final line")
    audit_ids: set[str] = set()
    for line_number, line in enumerate(payload.splitlines(), start=1):
        audit_id = _decode_audit_line(line, line_number=line_number)
        if audit_id in audit_ids:
            raise AuditLogCorruptedError(
                f"audit log contains duplicate audit_id {audit_id!r}"
            )
        audit_ids.add(audit_id)
    return audit_ids


def _project_safety_gate(gate: object) -> dict[str, object]:
    if type(gate) is not SafetyGateResult:
        raise ValueError("safety gates must be concrete SafetyGateResult values")
    try:
        return {"name": gate.name, "passed": gate.passed, "detail": gate.detail}
    except AttributeError as exc:
        raise ValueError("safety gate is malformed") from exc


def _project_evidence_provenance(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    if type(value) is not EvidenceProvenance:
        raise ValueError("evidence provenance must be concrete")
    try:
        return {
            "schema_version": value.schema_version,
            "metric": value.metric.value,
            "threshold_stratum": value.threshold_stratum,
            "reference_window_id": value.reference_window_id,
            "current_window_id": value.current_window_id,
            "reference_manifest_sha256": value.reference_manifest_sha256,
            "current_manifest_sha256": value.current_manifest_sha256,
            "configuration_identity": value.configuration_identity,
            "data_identity": value.data_identity,
            "flat_binding_id": value.flat_binding_id,
            "hnsw_binding_id": value.hnsw_binding_id,
            "reference_audit_ids": list(value.reference_audit_ids),
            "reference_audit_rank_digests": list(
                value.reference_audit_rank_digests
            ),
            "current_audit_ids": list(value.current_audit_ids),
            "current_audit_rank_digests": list(value.current_audit_rank_digests),
            "sha256": value.sha256,
        }
    except AttributeError as exc:
        raise ValueError("evidence provenance is malformed") from exc


def _project_rollback_verification(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    if type(value) is not RollbackVerification:
        raise ValueError("rollback verification must be concrete")
    try:
        return {
            "success": value.success,
            "restored_ef": value.restored_ef,
            "health_passed": value.health_passed,
            "audit_passed": value.audit_passed,
            "configuration_identity": value.configuration_identity,
            "index_identity": value.index_identity,
            "data_identity": value.data_identity,
            "detail": value.detail,
        }
    except AttributeError as exc:
        raise ValueError("rollback verification is malformed") from exc


def _project_common_context(context: object, *, kind: str) -> dict[str, object]:
    try:
        metric = Metric(context.metric).value  # type: ignore[attr-defined]
        return {
            "context_schema_version": ACTUATION_CONTEXT_SCHEMA_VERSION,
            "context_kind": kind,
            "metric": metric,
            "threshold_stratum": context.threshold_stratum,  # type: ignore[attr-defined]
            "collection_name": context.collection_name,  # type: ignore[attr-defined]
            "configuration_identity": context.configuration_identity,  # type: ignore[attr-defined]
            "index_identity": context.index_identity,  # type: ignore[attr-defined]
            "flat_index_identity": context.flat_index_identity,  # type: ignore[attr-defined]
            "data_identity": context.data_identity,  # type: ignore[attr-defined]
            "occurred_at_utc": context.occurred_at_utc,  # type: ignore[attr-defined]
        }
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("actuation context is malformed") from exc


def _project_v3_context(record: ActuationAuditRecord) -> dict[str, object]:
    if record.action is PolicyAction.ROLLBACK:
        if type(record.context) is not RollbackActuationContext:
            raise ValueError("ROLLBACK requires a concrete RollbackActuationContext")
        result = _project_common_context(record.context, kind="ROLLBACK")
        try:
            result.update(
                {
                    "expected_last_known_good_ef": (
                        record.context.expected_last_known_good_ef
                    ),
                    "audited_query_ids": list(record.context.audited_query_ids),
                }
            )
        except AttributeError as exc:
            raise ValueError("rollback context is malformed") from exc
        return result
    if type(record.context) is not ActuationIdentityContext:
        raise ValueError("policy actions require a concrete ActuationIdentityContext")
    return _project_common_context(record.context, kind="POLICY")


def _project_v3_record(record: object) -> dict[str, object]:
    if type(record) is not ActuationAuditRecord:
        raise TypeError("record must be a concrete ActuationAuditRecord")
    try:
        if type(record.action) is not PolicyAction:
            raise ValueError("record action must be a concrete PolicyAction")
        if type(record.outcome) is not ActuationOutcome:
            raise ValueError("record outcome must be a concrete ActuationOutcome")
        if record.shadow_result is not None or record.canary_observation is not None:
            raise ValueError("v3 generic audit cannot contain shadow or canary results")
        projected = {
            "audit_id": record.audit_id,
            "action": record.action.value,
            "outcome": record.outcome.value,
            "attempted": record.attempted,
            "success": record.success,
            "reason": record.reason,
            "context": _project_v3_context(record),
            "current_ef": record.current_ef,
            "candidate_ef": record.candidate_ef,
            "last_known_good_ef": record.last_known_good_ef,
            "traffic_fraction": record.traffic_fraction,
            "policy_reason": record.policy_reason,
            "safety_gate_results": [
                _project_safety_gate(gate) for gate in record.safety_gate_results
            ],
            "shadow_result": None,
            "canary_observation": None,
            "rollback_verification": _project_rollback_verification(
                record.rollback_verification
            ),
            "automatic_actions_disabled": record.automatic_actions_disabled,
            "evidence_provenance": _project_evidence_provenance(
                record.evidence_provenance
            ),
        }
        _validate_v3_record(projected)
        return projected
    except AuditLogCorruptedError as exc:
        raise ValueError(f"record violates audit-v3 contract: {exc}") from exc
    except AttributeError as exc:
        raise ValueError("actuation audit record is malformed") from exc


class JsonlAuditSink:
    """Process-safe mixed-v2/v3 reader and schema-v3-only audit writer."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def contains(self, audit_id: str) -> bool:
        """Return whether ``audit_id`` exists, raising on any corrupt line."""

        if not _nonempty(audit_id):
            raise ValueError("audit_id must be non-empty")
        try:
            handle = self.path.open("r", encoding="utf-8", newline="")
        except FileNotFoundError:
            return False
        except (OSError, UnicodeError) as exc:
            raise AuditLogCorruptedError("audit log is unreadable") from exc
        with handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
            try:
                return audit_id in _scan_audit_handle(handle)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def append(self, record: ActuationAuditRecord) -> None:
        """Lock, duplicate-check, append once, flush, and fsync one record."""

        projected = _project_v3_record(record)
        audit_id = projected["audit_id"]
        assert isinstance(audit_id, str)
        if not self.path.parent.is_dir():
            raise FileNotFoundError(
                f"parent directory does not exist: {self.path.parent}"
            )
        envelope = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "record": projected,
        }
        serialized = json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

        descriptor = os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        with os.fdopen(descriptor, "a+", encoding="utf-8", newline="") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                audit_ids = _scan_audit_handle(handle)
                if audit_id in audit_ids:
                    raise DuplicateAuditIdError(
                        f"duplicate audit_id: {audit_id}"
                    )
                handle.seek(0, os.SEEK_END)
                handle.write(serialized + "\n")
                handle.flush()
                os.fsync(handle.fileno())
                _fsync_parent(self.path)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _fsync_parent(path: Path) -> None:
    directory_descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    parent = path.parent
    if not parent.is_dir():
        raise FileNotFoundError(f"parent directory does not exist: {parent}")
    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=parent,
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = -1
            handle.write(serialized + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_parent(path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _decode_controller_state(path: Path) -> dict[str, Any] | None:
    try:
        payload = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        decoded = json.loads(payload, object_pairs_hook=_object_without_duplicate_keys)
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        _DuplicateJsonField,
        TypeError,
        ValueError,
    ) as exc:
        raise ValueError("controller state is malformed") from exc
    if not _exact_mapping(decoded, _CONTROLLER_FIELDS):
        raise ValueError("controller state fields do not match schema")
    assert isinstance(decoded, Mapping)
    if (
        type(decoded["schema_version"]) is not int
        or decoded["schema_version"] != CONTROLLER_SCHEMA_VERSION
    ):
        raise ValueError("unsupported controller schema version")
    if decoded["state"] not in {"DISABLED", "ENABLED"}:
        raise ValueError("controller state value is invalid")
    if not _nonempty(decoded["reason"]):
        raise ValueError("controller reason is empty")
    if not _valid_rfc3339_utc(decoded["changed_at_utc"]):
        raise ValueError("controller timestamp is invalid")
    if decoded["state"] == "DISABLED":
        if not _nonempty(decoded["audit_id"]) or decoded["confirmed_by"] is not None:
            raise ValueError("disabled controller identity is invalid")
    elif not _nonempty(decoded["confirmed_by"]):
        raise ValueError("enabled controller confirmation identity is invalid")
    if decoded["audit_id"] is not None and not _nonempty(decoded["audit_id"]):
        raise ValueError("controller audit identity is invalid")
    return dict(decoded)


class FileAutomaticActionController:
    """Atomic, restart-durable automatic-action state with human re-enable."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        clock: Clock = _current_rfc3339_utc,
    ) -> None:
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.path = Path(path)
        self._clock = clock

    def _timestamp(self) -> str:
        timestamp = self._clock()
        if not _valid_rfc3339_utc(timestamp):
            raise ValueError("clock must return an RFC3339 UTC timestamp ending Z")
        return timestamp

    def disable_automatic_actions(self, *, audit_id: str, reason: str) -> None:
        """Atomically persist disabled state for the triggering audit record."""

        if not _nonempty(audit_id):
            raise ValueError("audit_id must be non-empty")
        if not _nonempty(reason):
            raise ValueError("reason must be non-empty")
        _atomic_write_json(
            self.path,
            {
                "schema_version": CONTROLLER_SCHEMA_VERSION,
                "state": "DISABLED",
                "audit_id": audit_id,
                "reason": reason,
                "changed_at_utc": self._timestamp(),
                "confirmed_by": None,
            },
        )

    def is_disabled(self) -> bool:
        """Return false only for missing or strictly valid enabled state."""

        try:
            state = _decode_controller_state(self.path)
        except (OSError, ValueError):
            return True
        return state is not None and state["state"] == "DISABLED"

    def re_enable(
        self,
        *,
        confirmation: str,
        confirmed_by: str,
        reason: str,
    ) -> None:
        """Explicitly re-enable after exact human confirmation."""

        if confirmation != REENABLE_CONFIRMATION_TOKEN:
            raise ValueError("exact human confirmation token is required")
        if not _nonempty(confirmed_by):
            raise ValueError("confirmed_by must be non-empty")
        if not _nonempty(reason):
            raise ValueError("reason must be non-empty")
        try:
            prior = _decode_controller_state(self.path)
        except (OSError, ValueError):
            prior = None
        _atomic_write_json(
            self.path,
            {
                "schema_version": CONTROLLER_SCHEMA_VERSION,
                "state": "ENABLED",
                "audit_id": None if prior is None else prior["audit_id"],
                "reason": reason,
                "changed_at_utc": self._timestamp(),
                "confirmed_by": confirmed_by,
            },
        )


__all__ = [
    "ACTUATION_CONTEXT_SCHEMA_VERSION",
    "AUDIT_SCHEMA_VERSION",
    "CONTROLLER_SCHEMA_VERSION",
    "HISTORICAL_AUDIT_SCHEMA_VERSION",
    "REENABLE_CONFIRMATION_TOKEN",
    "AuditLogCorruptedError",
    "DuplicateAuditIdError",
    "FileAutomaticActionController",
    "JsonlAuditSink",
]
