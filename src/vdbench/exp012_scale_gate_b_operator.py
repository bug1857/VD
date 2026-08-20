"""Scale-specific Gate-B operator for the two governed EXP-012 profiles.

The implementation reuses the accepted loopback ingress and serving boundary,
but emits only EXP-012 plan/result schemas.  It does not broaden EXP-010's
frozen 600-source contract or its evidence namespace.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical_serialization import strict_canonical_digest, strict_canonical_json_bytes
from .exp010_gate_b_operator import (
    Exp010GateBOperands,
    Exp010GateBOperatorError,
    build_gate_b_plan as build_exp010_gate_b_plan,
    run_gate_b_host_from_cli,
)
from .exp012_scale_contract import (
    Exp012ScaleContract,
    Exp012ScaleProfile,
    build_exp012_scale_contract,
    exp012_scale_contract_payload,
    verify_exp012_scale_contract,
)
from .exp012_scale_campaign import (
    Exp012ScaleCampaignError,
    load_scale_campaign_marker,
    marker_path,
    write_scale_campaign_marker,
)

__all__ = [
    "EXP012_GATE_B_PLAN_SCHEMA_VERSION",
    "Exp012ScaleGateBOperands",
    "Exp012ScaleGateBOperatorError",
    "build_gate_b_plan",
    "load_operands",
    "main",
    "run_gate_b",
]


EXP012_GATE_B_PLAN_SCHEMA_VERSION = "exp012-scale-gate-b-plan-v1"
_RESULT_SCHEMA_VERSION = "exp012-scale-gate-b-result-v1"
_PLAN_DOMAIN = b"VD::EXP012_SCALE_GATE_B_PLAN::V1\x00"
_RESULT_DOMAIN = b"VD::EXP012_SCALE_GATE_B_RESULT::V1\x00"
_FIELDS = (
    "campaign_root",
    "gate_a_campaign_root",
    "scale_profile",
    "detector_seed",
    "host_address",
    "host_port",
    "etcd_container",
    "minio_container",
)


class Exp012ScaleGateBOperatorError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> Exp012ScaleGateBOperatorError:
    return Exp012ScaleGateBOperatorError(code)


@dataclass(frozen=True, slots=True)
class Exp012ScaleGateBOperands:
    contract: Exp012ScaleContract
    base: Exp010GateBOperands
    gate_a_campaign_root: Path


def _text(values: Mapping[str, object], name: str) -> str:
    value = values[name]
    if type(value) is not str or not value or value != value.strip():
        raise _error("EXP012_GATE_B_OPERAND_INVALID")
    return value


def _integer(values: Mapping[str, object], name: str) -> int:
    value = values[name]
    if type(value) is not int:
        raise _error("EXP012_GATE_B_OPERAND_INVALID")
    return value


def load_operands(path: str | os.PathLike[str]) -> Exp012ScaleGateBOperands:
    try:
        values = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _error("EXP012_GATE_B_OPERANDS_MALFORMED") from exc
    if type(values) is not dict or set(values) != set(_FIELDS):
        raise _error("EXP012_GATE_B_OPERANDS_INVALID")
    try:
        profile = Exp012ScaleProfile(_text(values, "scale_profile"))
    except ValueError as exc:
        raise _error("EXP012_GATE_B_PROFILE_INVALID") from exc
    contract = build_exp012_scale_contract(profile)
    campaign_root = Path(_text(values, "campaign_root"))
    gate_a_campaign_root = Path(_text(values, "gate_a_campaign_root"))
    host_address = _text(values, "host_address")
    if host_address not in {"127.0.0.1", "::1", "localhost"}:
        raise _error("EXP012_GATE_B_HOST_NOT_LOOPBACK")
    host_port = _integer(values, "host_port")
    if not 1 <= host_port <= 65535:
        raise _error("EXP012_GATE_B_OPERAND_INVALID")
    from .exp010_gate_b_operator import _inherit_gate_a_authority

    authority, deployment_identity, data_identity, gate_a_digest = (
        _inherit_gate_a_authority(gate_a_campaign_root)
    )
    base = Exp010GateBOperands(
        campaign_root=campaign_root,
        detector_seed=_integer(values, "detector_seed"),
        host_address=host_address,
        host_port=host_port,
        target_source_records=contract.target_source_records,
        etcd_container=_text(values, "etcd_container"),
        minio_container=_text(values, "minio_container"),
        authority=authority,
        deployment_identity=deployment_identity,
        data_identity=data_identity,
        gate_a_evidence_sha256=gate_a_digest,
    )
    # Reuse the accepted validator for loopback address, port, target shape,
    # and authority consistency by planning before any execute path.
    return Exp012ScaleGateBOperands(
        contract=contract,
        base=base,
        gate_a_campaign_root=gate_a_campaign_root,
    )


def _verify_external_gate_a_authority(operands: Exp012ScaleGateBOperands) -> None:
    """Rebind even manually supplied operands to the independent Gate-A root."""

    from .exp010_gate_b_operator import _inherit_gate_a_authority

    try:
        authority, deployment_identity, data_identity, gate_a_digest = (
            _inherit_gate_a_authority(operands.gate_a_campaign_root)
        )
        supplied_authority = strict_canonical_json_bytes(dict(operands.base.authority))
        verified_authority = strict_canonical_json_bytes(dict(authority))
    except (Exp010GateBOperatorError, TypeError, ValueError) as exc:
        raise _error("EXP012_GATE_B_GATE_A_AUTHORITY_INVALID") from exc
    if (
        supplied_authority != verified_authority
        or type(operands.base.deployment_identity) is not str
        or operands.base.deployment_identity != deployment_identity
        or type(operands.base.data_identity) is not str
        or operands.base.data_identity != data_identity
        or type(operands.base.gate_a_evidence_sha256) is not str
        or operands.base.gate_a_evidence_sha256 != gate_a_digest
    ):
        raise _error("EXP012_GATE_B_GATE_A_AUTHORITY_MISMATCH")


def build_gate_b_plan(operands: Exp012ScaleGateBOperands) -> dict[str, object]:
    if type(operands) is not Exp012ScaleGateBOperands:
        raise _error("EXP012_GATE_B_OPERANDS_INVALID")
    contract = verify_exp012_scale_contract(operands.contract)
    if (
        type(operands.base) is not Exp010GateBOperands
        or operands.base.target_source_records != contract.target_source_records
        or not isinstance(operands.gate_a_campaign_root, Path)
        or operands.gate_a_campaign_root == operands.base.campaign_root
    ):
        raise _error("EXP012_GATE_B_OPERANDS_INVALID")
    _verify_external_gate_a_authority(operands)
    base = build_exp010_gate_b_plan(
        operands.base,
        accepted_scale_contract_sha256=contract.contract_sha256,
        authority_campaign_root=operands.gate_a_campaign_root,
    )
    marker = marker_path(operands.base.campaign_root)
    if marker.exists():
        load_scale_campaign_marker(
            operands.base.campaign_root,
            expected_contract=contract,
            expected_gate_a_evidence_sha256=operands.base.gate_a_evidence_sha256,
        )
    elif base["restart"]["stores_present"]:
        raise _error("EXP012_GATE_B_CAMPAIGN_MARKER_MISSING")
    source_target = base["source_target"]
    plan: dict[str, object] = {
        "schema_version": EXP012_GATE_B_PLAN_SCHEMA_VERSION,
        "experiment_id": "EXP-012-SCALE",
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "canonical_ingress": base["canonical_ingress"],
        "canonical_boundary": base["canonical_boundary"],
        "campaign": base["campaign"],
        "scale_campaign_marker": {
            "path": str(marker),
            "present": marker.exists(),
            "would_create": not marker.exists(),
        },
        "gate_a": {
            **base["gate_a"],
            "authority_campaign_root": str(operands.gate_a_campaign_root),
        },
        "stream": base["stream"],
        "serving": base["serving"],
        "detector_seed": base["detector_seed"],
        "endpoint": base["endpoint"],
        "source_target": source_target,
        "restart": base["restart"],
        "gate_c": base["gate_c"],
        "source_revision": base["source_revision"],
        "physical_searches_issued_by_preflight": 0,
        "serve_calls_issued_by_preflight": 0,
    }
    plan["plan_sha256"] = strict_canonical_digest(_PLAN_DOMAIN, plan)
    return plan


def run_gate_b(operands: Exp012ScaleGateBOperands) -> dict[str, object]:
    plan = build_gate_b_plan(operands)
    return _run_gate_b_after_plan(operands, plan)


def _run_gate_b_after_plan(
    operands: Exp012ScaleGateBOperands, plan: dict[str, object]
) -> dict[str, object]:
    contract = operands.contract
    if plan.get("scale_contract_sha256") != contract.contract_sha256:
        raise _error("EXP012_GATE_B_PLAN_MISMATCH")
    write_scale_campaign_marker(
        operands.base.campaign_root,
        contract,
        gate_a_evidence_sha256=operands.base.gate_a_evidence_sha256,
    )
    base_result = run_gate_b_host_from_cli(
        operands.base,
        accepted_scale_contract_sha256=contract.contract_sha256,
    )
    if (
        base_result.get("durable_source_records") != contract.target_source_records
        or base_result.get("complete_windows") != contract.expected_windows
    ):
        raise _error("EXP012_GATE_B_TARGET_NOT_COMPLETE")
    result: dict[str, object] = {
        "schema_version": _RESULT_SCHEMA_VERSION,
        "experiment_id": "EXP-012-SCALE",
        "scale_contract_sha256": contract.contract_sha256,
        "durable_source_records": contract.target_source_records,
        "complete_windows": contract.expected_windows,
        "projected_physical_searches": contract.expected_physical_searches,
        "gate_c": base_result["gate_c"],
    }
    result["result_sha256"] = strict_canonical_digest(_RESULT_DOMAIN, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operands", type=Path, required=True)
    parser.add_argument("--mode", choices=("preflight", "execute"), required=True)
    parser.add_argument("--confirm-gate-b-ingress", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        operands = load_operands(args.operands)
        plan = build_gate_b_plan(operands)
        sys.stdout.write(strict_canonical_json_bytes(plan).decode() + os.linesep)
        if args.mode == "preflight":
            return 0
        if not args.confirm_gate_b_ingress:
            raise _error("EXP012_GATE_B_CONFIRMATION_REQUIRED")
        result = _run_gate_b_after_plan(operands, plan)
        sys.stdout.write(strict_canonical_json_bytes(result).decode() + os.linesep)
        return 0
    except (
        Exp012ScaleGateBOperatorError,
        Exp010GateBOperatorError,
        Exp012ScaleCampaignError,
    ) as exc:
        sys.stderr.write(f"{exc.code}: {exc}{os.linesep}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
