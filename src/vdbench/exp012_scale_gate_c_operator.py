"""Scale-specific Gate-C operator with durable per-search telemetry.

The accepted detector/attempt/acknowledgement/attestation/finalization pipeline
is reused unchanged.  EXP-012 adds only a distinct plan/result namespace,
exact scale completion checks, and an additive telemetry ledger.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from .canonical_serialization import strict_canonical_digest, strict_canonical_json_bytes
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
from .host_window_lineage import HostResponseCommitError, SQLiteHostResponseCommitStore
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
    "load_operands",
    "main",
    "run_gate_c",
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operands", type=Path, required=True)
    parser.add_argument("--mode", choices=("preflight", "execute"), required=True)
    parser.add_argument("--confirm-physical-shadow-searches", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        operands = load_operands(args.operands)
        plan = build_gate_c_plan(operands)
        sys.stdout.write(strict_canonical_json_bytes(plan).decode() + os.linesep)
        if args.mode == "preflight":
            return 0
        if not args.confirm_physical_shadow_searches:
            raise _error("EXP012_GATE_C_CONFIRMATION_REQUIRED")
        result = _run_gate_c_after_plan(operands, plan)
        sys.stdout.write(strict_canonical_json_bytes(result).decode() + os.linesep)
        return 0
    except (
        Exp012ScaleGateCOperatorError,
        Exp010GateCOperatorError,
        HostResponseCommitError,
        ShadowSearchTelemetryError,
    ) as exc:
        sys.stderr.write(f"{exc.code}: {exc}{os.linesep}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
