"""The committed canonical Gate-C operator entrypoint for EXP-010.

Why this module exists:
    `Exp010LiveRunner.process_ready_windows()` has always been the canonical
    Gate-C call, but no committed code constructed the production composition
    around it. Every historical campaign was therefore driven by an ad-hoc,
    unpersisted composition, and a later preflight could not prove which path
    had actually run. This module is that missing entrypoint, and it is
    deliberately the *smallest* one that can be audited: it invents no runner,
    no window logic, no comparator, and no detector.

What it is NOT:
    Not a workload generator. `serve(...)` is never called here -- Gate B's
    genuine ingress is `exp010_ingress`, a separate boundary. This entrypoint
    only advances windows whose source membership is already durable, so it
    cannot manufacture, replay, or re-serve a single query.

Two modes, never one:
    `--mode preflight` constructs and cross-validates everything, opens the
    already-initialized stores, prints the resolved canonical plan, and issues
    zero physical searches -- it injects executors that raise if called, so a
    logic error cannot silently reach Milvus. `--mode execute` re-runs that
    entire preflight first and then, only after a second explicit operator
    flag, builds the real read-only client and calls `process_ready_windows()`.

Fail-closed configuration:
    The operand file has one exact, closed key set; a missing or unexpected key
    is refused rather than defaulted. `configuration_identity` is not merely
    syntax-checked -- it is re-derived from the serving operands and must match,
    so a stream cannot be advanced under an identity its own configuration does
    not produce. Store bindings are verified by opening the stores, which
    happens before any Milvus client exists.

Recovery:
    Ambiguous STARTED attempts are not this module's business to resolve. It
    never retries, never replays, and never converts an orphan into progress:
    `process_ready_windows()` raises and that reason code is reported verbatim.

Authority:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created or imported.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .canonical_serialization import (
    CANONICAL_JSON_SCHEMA_VERSION,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from .config import Metric
from .exp010_live_runner import (
    Exp010LiveRunner,
    Exp010OperatorConfiguration,
    Exp010WindowResult,
)
from .exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
    validate_governed_configuration_identity,
)
from .exp010_v2_host import SHADOW_CONSUMER_ID
from .shadow_window import TRACE_COUNT, WINDOW_QUERY_COUNT

__all__ = [
    "GATE_C_PLAN_SCHEMA_VERSION",
    "OPERAND_FIELDS",
    "Exp010GateCOperands",
    "Exp010GateCOperatorError",
    "MonotonicUtcClock",
    "build_gate_c_plan",
    "load_operands",
    "main",
    "run_gate_c_execute_from_cli",
]


GATE_C_PLAN_SCHEMA_VERSION = "exp010-gate-c-plan-v1"
_PLAN_DOMAIN = b"VD::EXP010_GATE_C_PLAN::V1\x00"

#: The exact, closed operand set. Anything else is refused, never defaulted.
OPERAND_FIELDS = (
    "stream_id",
    "metric",
    "threshold_stratum",
    "threshold_radius",
    "range_filter",
    "limit",
    "served_ef",
    "dimensions",
    "consistency_level",
    "configuration_identity",
    "flat_binding_id",
    "hnsw_binding_id",
    "source_revision",
    "environment_manifest_sha256",
    "detector_seed",
    "milvus_uri",
    "flat_collection_name",
    "hnsw_collection_name",
    "store_root",
    "dataset001_dir",
    "exp010_output_dir",
    "etcd_container",
    "minio_container",
)

#: Every store the composition owns. All must already exist: Gate C advances an
#: initialized campaign and never brings one into being.
_REQUIRED_STORES = (
    "v2_source.sqlite3",
    "v2_shadow_attempts.sqlite3",
    "v2_detector.sqlite3",
    "v2_attestation.sqlite3",
    "v2_window_finalization.sqlite3",
)

_SHA256_LENGTH = 64
_ONE_MICROSECOND = timedelta(microseconds=1)


class Exp010GateCOperatorError(RuntimeError):
    """Fail-closed operator error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010GateCOperatorError:
    return Exp010GateCOperatorError(code, message)


class MonotonicUtcClock:
    """Strictly increasing RFC3339 UTC timestamps for durable boundaries.

    FINDING-008: UTC is the *audit* timestamp, never causal authority --
    elapsed-time authority is `time.perf_counter_ns` inside the capture path
    and is untouched here. This clock exists only because the window assembler
    requires strictly increasing envelope timestamps; if the wall clock repeats
    or steps backward, the previous value is advanced by one microsecond rather
    than allowing a non-increasing durable record.
    """

    def __init__(self, now=None) -> None:
        self._now = now or (lambda: datetime.now(UTC))
        self._last: datetime | None = None

    def __call__(self) -> str:
        value = self._now()
        if value.tzinfo is None:
            raise _error("GATE_C_CLOCK_INVALID", "clock must be timezone-aware UTC")
        value = value.astimezone(UTC)
        if self._last is not None and value <= self._last:
            value = self._last + _ONE_MICROSECOND
        self._last = value
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class _RefusingServingExecutor:
    """Any call is a bug: Gate C never serves a genuine request."""

    def execute(self, request: object) -> Any:
        raise _error(
            "GATE_C_SERVING_FORBIDDEN",
            "the Gate-C entrypoint must never serve a workload request",
        )


class _RefusingShadowCaptureExecutor:
    """Any call is a bug: preflight issues zero physical searches."""

    def capture(self, sources: object, *, trace_sequence_index: int) -> Any:
        raise _error(
            "GATE_C_PREFLIGHT_CAPTURE_FORBIDDEN",
            "preflight must never issue a physical shadow search",
        )


@dataclass(frozen=True, slots=True)
class Exp010GateCOperands:
    """One fully validated, self-consistent Gate-C operand set."""

    stream_id: str
    metric: Metric
    threshold_stratum: str
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int
    dimensions: int
    consistency_level: str
    configuration_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    source_revision: str
    environment_manifest_sha256: str
    detector_seed: int
    milvus_uri: str
    flat_collection_name: str
    hnsw_collection_name: str
    store_root: Path
    dataset001_dir: Path
    exp010_output_dir: Path
    etcd_container: str
    minio_container: str

    @property
    def serving_configuration(self) -> Exp010ServingConfiguration:
        return Exp010ServingConfiguration(
            metric=self.metric,
            threshold_stratum=self.threshold_stratum,
            threshold_radius=self.threshold_radius,
            range_filter=self.range_filter,
            limit=self.limit,
            served_ef=self.served_ef,
            dimensions=self.dimensions,
            consistency_level=self.consistency_level,
        )

    def runner_configuration(self) -> Exp010OperatorConfiguration:
        return Exp010OperatorConfiguration(
            milvus_uri=self.milvus_uri,
            flat_collection_name=self.flat_collection_name,
            hnsw_collection_name=self.hnsw_collection_name,
            metric=self.metric,
            threshold_stratum=self.threshold_stratum,
            threshold_radius=self.threshold_radius,
            served_ef=self.served_ef,
            detector_seed=self.detector_seed,
            stream_id=self.stream_id,
            configuration_identity=self.configuration_identity,
            flat_binding_id=self.flat_binding_id,
            hnsw_binding_id=self.hnsw_binding_id,
            source_revision=self.source_revision,
            environment_manifest_sha256=self.environment_manifest_sha256,
            store_root=self.store_root,
            dataset001_dir=self.dataset001_dir,
            exp010_output_dir=self.exp010_output_dir,
            consistency_level=self.consistency_level,
        )


def _text(values: Mapping[str, Any], name: str) -> str:
    value = values[name]
    if type(value) is not str or not value or value != value.strip():
        raise _error("GATE_C_OPERAND_INVALID", name)
    return value


def _exact_int(values: Mapping[str, Any], name: str) -> int:
    value = values[name]
    if isinstance(value, bool) or type(value) is not int:
        raise _error("GATE_C_OPERAND_INVALID", name)
    return value


def _real(values: Mapping[str, Any], name: str) -> float:
    value = values[name]
    if isinstance(value, bool) or type(value) not in (int, float):
        raise _error("GATE_C_OPERAND_INVALID", name)
    return float(value)


def _sha256(values: Mapping[str, Any], name: str) -> str:
    value = _text(values, name)
    if len(value) != _SHA256_LENGTH or any(
        char not in "0123456789abcdef" for char in value
    ):
        raise _error("GATE_C_OPERAND_INVALID", name)
    return value


def load_operands(path: str | os.PathLike[str]) -> Exp010GateCOperands:
    """Load and fully cross-validate one operand file, contacting nothing.

    Every operand is required. The single most important check here is that
    `configuration_identity` is *re-derived* from the serving operands rather
    than trusted: a stream can never be advanced under an identity its own
    configuration does not actually produce.
    """

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise _error("GATE_C_OPERANDS_UNREADABLE", str(exc)) from exc
    try:
        values = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("GATE_C_OPERANDS_MALFORMED", str(exc)) from exc
    if type(values) is not dict:
        raise _error("GATE_C_OPERANDS_MALFORMED", "operands must be a JSON object")
    missing = sorted(set(OPERAND_FIELDS) - set(values))
    if missing:
        raise _error("GATE_C_OPERANDS_INCOMPLETE", ",".join(missing))
    unexpected = sorted(set(values) - set(OPERAND_FIELDS))
    if unexpected:
        raise _error("GATE_C_OPERANDS_UNEXPECTED", ",".join(unexpected))

    try:
        metric = Metric(_text(values, "metric"))
    except ValueError as exc:
        raise _error("GATE_C_OPERAND_INVALID", "metric") from exc

    # Syntax first (NEW_OBSERVATION_A), derivation second.
    configuration_identity = validate_governed_configuration_identity(
        values["configuration_identity"]
    )

    operands = Exp010GateCOperands(
        stream_id=_text(values, "stream_id"),
        metric=metric,
        threshold_stratum=_text(values, "threshold_stratum"),
        threshold_radius=_real(values, "threshold_radius"),
        range_filter=_real(values, "range_filter"),
        limit=_exact_int(values, "limit"),
        served_ef=_exact_int(values, "served_ef"),
        dimensions=_exact_int(values, "dimensions"),
        consistency_level=_text(values, "consistency_level"),
        configuration_identity=configuration_identity,
        flat_binding_id=_text(values, "flat_binding_id"),
        hnsw_binding_id=_text(values, "hnsw_binding_id"),
        source_revision=_text(values, "source_revision"),
        environment_manifest_sha256=_sha256(values, "environment_manifest_sha256"),
        detector_seed=_exact_int(values, "detector_seed"),
        milvus_uri=_text(values, "milvus_uri"),
        flat_collection_name=_text(values, "flat_collection_name"),
        hnsw_collection_name=_text(values, "hnsw_collection_name"),
        store_root=Path(_text(values, "store_root")),
        dataset001_dir=Path(_text(values, "dataset001_dir")),
        exp010_output_dir=Path(_text(values, "exp010_output_dir")),
        etcd_container=_text(values, "etcd_container"),
        minio_container=_text(values, "minio_container"),
    )

    derived = derive_serving_configuration_identity(operands.serving_configuration)
    if derived != operands.configuration_identity:
        raise _error(
            "GATE_C_CONFIGURATION_IDENTITY_MISMATCH",
            f"operands derive {derived}",
        )
    if operands.flat_collection_name == operands.hnsw_collection_name:
        raise _error("GATE_C_OPERAND_INVALID", "collection names must differ")
    if operands.flat_binding_id == operands.hnsw_binding_id:
        raise _error("GATE_C_OPERAND_INVALID", "binding ids must differ")
    return operands


def _require_initialized_stores(store_root: Path) -> tuple[str, ...]:
    """Gate C advances an initialized campaign; it never creates one."""

    if not store_root.is_dir():
        raise _error("GATE_C_STORE_ROOT_MISSING", str(store_root))
    absent = tuple(
        name for name in _REQUIRED_STORES if not (store_root / name).is_file()
    )
    if absent:
        raise _error("GATE_C_STORES_NOT_INITIALIZED", ",".join(absent))
    return _REQUIRED_STORES


def _require_verified_gate_a_authority(
    operands: Exp010GateCOperands,
    *,
    authority_campaign_root: Path | None = None,
) -> dict[str, Any]:
    """Bind this run to the Gate-A artifact rather than to a typed digest.

    ADR-017 item 9 makes authority flow one way, but until this check existed an
    operator could type any syntactically valid `environment_manifest_sha256`
    into the operand file and build a downstream chain that no Gate A had ever
    attested. The digest is now inherited, not asserted: the campaign root is
    derived from the existing `store_root` -- no operand is added, and the
    closed 23-key contract is unchanged -- and the persisted Gate-A evidence is
    canonically decoded and fully re-verified before its digest is compared.

    Verification is mandatory and is never skipped because evidence is absent:
    missing, malformed, incomplete, substituted, or mismatched Gate-A evidence
    all fail closed. Any future legacy allowance must be an explicit, versioned
    decision, never inferred from a missing file.
    """

    from .exp010_gate_a_operator import (
        Exp010GateAOperatorError,
        load_verified_gate_a_evidence,
    )

    campaign_root = (
        operands.store_root.parent
        if authority_campaign_root is None
        else Path(authority_campaign_root)
    )
    try:
        evidence = load_verified_gate_a_evidence(campaign_root)
    except Exp010GateAOperatorError as exc:
        raise _error(
            "GATE_C_GATE_A_AUTHORITY_UNVERIFIED", f"{campaign_root}: {exc.code}"
        ) from exc

    for label, code, attested, claimed in (
        (
            "environment_manifest_sha256",
            "GATE_C_ENVIRONMENT_AUTHORITY_MISMATCH",
            evidence["environment_manifest_sha256"],
            operands.environment_manifest_sha256,
        ),
        (
            "source_revision",
            "GATE_C_SOURCE_REVISION_AUTHORITY_MISMATCH",
            evidence["source_revision"],
            operands.source_revision,
        ),
        (
            "configuration_identity",
            "GATE_C_CONFIGURATION_AUTHORITY_MISMATCH",
            evidence["serving"]["configuration_identity"],
            operands.configuration_identity,
        ),
    ):
        if attested != claimed:
            raise _error(code, f"{label}: Gate A attests {attested}, operands claim {claimed}")
    return evidence


def _require_campaign_namespace(
    operands: Exp010GateCOperands,
    *,
    accepted_scale_contract_sha256: str | None,
) -> None:
    from .exp012_scale_campaign import (
        Exp012ScaleCampaignError,
        load_scale_campaign_marker,
        marker_path,
    )

    campaign_root = operands.store_root.parent
    if not marker_path(campaign_root).exists():
        if accepted_scale_contract_sha256 is not None:
            raise _error("GATE_C_EXP012_CAMPAIGN_MARKER_MISSING")
        return
    try:
        binding = load_scale_campaign_marker(campaign_root)
    except Exp012ScaleCampaignError as exc:
        raise _error("GATE_C_CAMPAIGN_NAMESPACE_INVALID", exc.code) from exc
    if accepted_scale_contract_sha256 != binding.contract.contract_sha256:
        raise _error("GATE_C_EXP012_REQUIRES_SCALE_OPERATOR")


def build_gate_c_plan(
    operands: Exp010GateCOperands,
    *,
    accepted_scale_contract_sha256: str | None = None,
    authority_campaign_root: Path | None = None,
) -> dict[str, object]:
    """Open the real stores read-only-ish and resolve the canonical plan.

    Contacts no service. The composition is built with executors that refuse
    every call, so this cannot issue a physical search or serve a request even
    if something downstream tried to. Opening the stores is what proves the
    operand identities match the campaign's own durable bindings, and it
    happens before any Milvus client can exist.
    """

    _require_campaign_namespace(
        operands,
        accepted_scale_contract_sha256=accepted_scale_contract_sha256,
    )
    _require_initialized_stores(operands.store_root)
    # Authority before work: the environment digest this run binds must be the
    # one a Gate A actually observed and persisted, not a typed string.
    gate_a_evidence = _require_verified_gate_a_authority(
        operands, authority_campaign_root=authority_campaign_root
    )
    clock = MonotonicUtcClock()
    runner = Exp010LiveRunner(
        configuration=operands.runner_configuration(),
        serving_executor=_RefusingServingExecutor(),
        shadow_capture_executor=_RefusingShadowCaptureExecutor(),
        clock=clock,
        shadow_captured_at_clock=clock,
    )
    try:
        composition = runner.composition
        next_window = composition.finalization_store.next_window_sequence()
        acknowledged = len(
            composition.response_store.consumer_acknowledgement_state(
                consumer_id=SHADOW_CONSUMER_ID
            ).event_ids
        )
        # Reopen already replayed and verified the full contiguous source
        # chain.  Bind the count to that verified source/outbox head instead of
        # replaying the entire chain once per window during planning.
        source_head = composition.response_store.verified_source_head()
        complete_windows = source_head.source_count // WINDOW_QUERY_COUNT
        plan: dict[str, object] = {
            "schema_version": GATE_C_PLAN_SCHEMA_VERSION,
            "canonical_entrypoint": (
                "vdbench.exp010_live_runner.Exp010LiveRunner.process_ready_windows"
            ),
            "canonical_composition": (
                "vdbench.exp010_v2_host.Exp010V2HostComposition"
            ),
            "canonical_capture_executor": (
                "vdbench.v2_milvus_shadow_capture.V2MilvusShadowCaptureExecutor"
            ),
            "serialization_contract": CANONICAL_JSON_SCHEMA_VERSION,
            "stream": {
                "stream_id": operands.stream_id,
                "metric": operands.metric.value,
                "threshold_stratum": operands.threshold_stratum,
                "configuration_identity": operands.configuration_identity,
                "data_identity": composition.data_identity,
                "flat_binding_id": operands.flat_binding_id,
                "hnsw_binding_id": operands.hnsw_binding_id,
            },
            "source_revision": operands.source_revision,
            "environment_manifest_sha256": operands.environment_manifest_sha256,
            "gate_a_authority": {
                "campaign_root": str(
                    operands.store_root.parent
                    if authority_campaign_root is None
                    else Path(authority_campaign_root)
                ),
                "evidence_sha256": gate_a_evidence["evidence_sha256"],
                "observed_at_utc": gate_a_evidence["observed_at_utc"],
            },
            "detector_seed": operands.detector_seed,
            "serving": {
                "threshold_radius": operands.threshold_radius,
                "range_filter": operands.range_filter,
                "limit": operands.limit,
                "served_ef": operands.served_ef,
                "dimensions": operands.dimensions,
                "consistency_level": operands.consistency_level,
            },
            "milvus": {
                "uri": operands.milvus_uri,
                "flat_collection_name": operands.flat_collection_name,
                "hnsw_collection_name": operands.hnsw_collection_name,
                "etcd_container": operands.etcd_container,
                "minio_container": operands.minio_container,
            },
            "stores": {
                "root": str(operands.store_root),
                "files": list(_REQUIRED_STORES),
            },
            "dataset001_dir": str(operands.dataset001_dir),
            "observed": {
                "shadow_acknowledged_count": acknowledged,
                "complete_source_windows": complete_windows,
                "next_window_sequence": next_window,
                "windows_pending": max(0, complete_windows - next_window),
            },
            "projected_physical_work": {
                "traces_per_window": TRACE_COUNT,
                "sources_per_window": WINDOW_QUERY_COUNT,
                "flat_searches": max(0, complete_windows - next_window)
                * WINDOW_QUERY_COUNT,
                "hnsw_sentinel_searches": max(0, complete_windows - next_window)
                * WINDOW_QUERY_COUNT,
            },
            "physical_searches_issued_by_preflight": 0,
            "serve_calls_issued_by_gate_c": 0,
        }
    finally:
        runner.composition.close()
    plan["plan_sha256"] = strict_canonical_digest(_PLAN_DOMAIN, plan)
    return plan


def run_gate_c_execute_from_cli(
    operands: Exp010GateCOperands,
    *,
    search_telemetry_store: Any | None = None,
    accepted_scale_contract_sha256: str | None = None,
) -> tuple[Exp010WindowResult, ...]:
    """The single real physical-execution seam.

    Reached only after `build_gate_c_plan` has already opened and validated
    every store against the operand identities, and only after the operator
    passed both `--mode execute` and the explicit confirmation flag. This
    repository never invokes it for real; tests patch this symbol to prove that
    preflight never reaches it and that a mismatched operand set never does.
    """

    _require_campaign_namespace(
        operands,
        accepted_scale_contract_sha256=accepted_scale_contract_sha256,
    )

    from .config import IndexTrack
    from .docker_health import DockerSocketHealthProbe
    from .milvus import MilvusHarness
    from .milvus_actuation import CollectionIdentityBinding
    from .v2_milvus_shadow_capture import (
        V2MilvusShadowCaptureExecutor,
        V2ShadowCaptureIdentityBinding,
        build_readonly_milvus_client,
    )

    client = build_readonly_milvus_client(operands.milvus_uri)
    stack_health_probe = DockerSocketHealthProbe(
        etcd_container=operands.etcd_container,
        minio_container=operands.minio_container,
    )
    harness = MilvusHarness(client, dimensions=operands.dimensions)
    identity_binding = V2ShadowCaptureIdentityBinding(
        flat_collection_name=operands.flat_collection_name,
        hnsw_collection_name=operands.hnsw_collection_name,
        flat_binding=CollectionIdentityBinding(
            identity_id=operands.flat_binding_id,
            expected=harness.index_identity(
                operands.flat_collection_name, operands.metric, IndexTrack.FLAT
            ),
        ),
        hnsw_binding=CollectionIdentityBinding(
            identity_id=operands.hnsw_binding_id,
            expected=harness.index_identity(
                operands.hnsw_collection_name, operands.metric, IndexTrack.HNSW
            ),
        ),
    )
    clock = MonotonicUtcClock()
    configuration = operands.runner_configuration()
    capture_executor = V2MilvusShadowCaptureExecutor(
        client=client,
        stream_key=_stream_key_for(configuration, operands),
        dataset001_dir=operands.dataset001_dir,
        identity_binding=identity_binding,
        threshold_radius=operands.threshold_radius,
        served_ef=operands.served_ef,
        source_revision=operands.source_revision,
        environment_manifest_sha256=operands.environment_manifest_sha256,
        stack_health_probe=stack_health_probe,
        occurred_at_clock=clock,
        search_telemetry_store=search_telemetry_store,
    )
    runner = Exp010LiveRunner(
        configuration=configuration,
        # Gate C never serves; a serving call here is a defect, not a fallback.
        serving_executor=_RefusingServingExecutor(),
        shadow_capture_executor=capture_executor,
        clock=clock,
        shadow_captured_at_clock=clock,
    )
    try:
        # The one canonical Gate-C call. No retry, no replay, no recovery
        # override: an ambiguous STARTED propagates its reason code verbatim.
        return runner.process_ready_windows()
    finally:
        runner.composition.close()


def _stream_key_for(configuration: Exp010OperatorConfiguration, operands):
    """Derive the stream key exactly as the composition does, without reopening."""

    from .exp010_v2_host import pin_dataset001_identity
    from .shadow_event_types import MonitorStreamKey

    dataset = pin_dataset001_identity(operands.dataset001_dir)
    return MonitorStreamKey(
        configuration.stream_id,
        configuration.metric,
        configuration.threshold_stratum,
        configuration.configuration_identity,
        dataset.data_identity,
        configuration.flat_binding_id,
        configuration.hnsw_binding_id,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operands",
        type=Path,
        required=True,
        help="Path to the exact-keyed Gate-C operand JSON file.",
    )
    parser.add_argument(
        "--mode",
        choices=("preflight", "execute"),
        required=True,
        help=(
            "preflight resolves and prints the plan and issues zero physical "
            "searches; execute additionally runs process_ready_windows()."
        ),
    )
    parser.add_argument(
        "--confirm-physical-shadow-searches",
        action="store_true",
        help=(
            "Required with --mode execute. A second, explicit operator action, "
            "deliberately separate from choosing the mode, acknowledging that "
            "real FLAT and sentinel-ef searches will be issued."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Real operator entry point; never invoked by this repository's own code.

    Ordering is the contract: operands are validated, the stores are opened and
    proven to match them, and the plan is printed -- all before any Milvus
    client can be constructed. A configuration or binding mismatch therefore
    always fails with zero physical capture.
    """

    args = _parser().parse_args(argv)
    operands = load_operands(args.operands)
    plan = build_gate_c_plan(operands)
    sys.stdout.write(strict_canonical_json_bytes(plan).decode("utf-8"))

    if args.mode == "preflight":
        return 0
    if not args.confirm_physical_shadow_searches:
        raise _error(
            "GATE_C_EXECUTION_NOT_CONFIRMED",
            "--mode execute requires --confirm-physical-shadow-searches",
        )
    results = run_gate_c_execute_from_cli(operands)
    summary = {
        "schema_version": GATE_C_PLAN_SCHEMA_VERSION,
        "plan_sha256": plan["plan_sha256"],
        "windows_processed": len(results),
        "windows": [
            {
                "window_sequence": item.window_sequence,
                "status": item.status.value,
                "reason_codes": list(item.reason_codes),
                "detector_state": (
                    None if item.detector_state is None else item.detector_state.value
                ),
                "attested": bool(item.attested),
            }
            for item in results
        ],
    }
    sys.stdout.write(strict_canonical_json_bytes(summary).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
