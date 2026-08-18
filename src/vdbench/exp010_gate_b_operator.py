"""The committed canonical Gate-B operator entrypoint for EXP-010.

Why this module exists:
    `Exp010RequestIngress.admit` onto `Exp010LiveRunner.serve` has always been
    the canonical Gate-B boundary, but no committed code hosted it. Every
    historical campaign was therefore driven by an ad-hoc, unpersisted operator
    composition, and a later preflight could not prove which host had actually
    accepted the genuine requests. This module is that missing host, and it is
    deliberately the *smallest* one that can be audited: it invents no runner,
    no ingress, no sequencing protocol, and no serving path.

What it is NOT:
    Not a workload generator. This module contains no vector sampler, no random
    source, no DATASET replay, no historical trace replay, and no benchmark
    generator. It never constructs a `query_vector`. Every admitted request
    exists solely because an external application posted it. `serve(...)` is
    reached only through the committed ingress, and only from an inbound
    request.

Gate separation is structural, not documentary:
    `process_ready_windows()` is never called, never imported, and cannot be
    reached from this entrypoint. The composition is additionally built with a
    *refusing* shadow-capture executor, so a Gate-C physical shadow search is
    impossible here even if a future defect tried: the executor raises. Gate C
    remains `exp010_gate_c_operator`, and Gates D/E remain operator-controlled.

Two modes, never one:
    `--mode preflight` verifies Gate-A authority, derives every governed
    identity from it, checks Milvus health and the live collection identities,
    proves the endpoint is bindable, resolves the canonical store paths, and
    prints the plan with a `plan_sha256`. It creates no Gate-B store, starts no
    listening server, and issues zero searches. `--mode execute` re-runs that
    entire preflight and then, only after a second explicit operator flag,
    binds loopback and serves genuine external requests.

Authority is inherited, never typed:
    Gate-A evidence is canonically re-verified and every governed identity --
    stream id, configuration identity, data identity, deployment identity,
    binding ids, environment digest, source revision, and the whole serving
    configuration -- is derived from it. The operand file cannot restate them,
    so an operator cannot host a campaign under an identity no Gate A attested.

Authority created here:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created or imported.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
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
from .exp010_live_runner import Exp010LiveRunner, Exp010OperatorConfiguration
from .exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from .shadow_window import WINDOW_QUERY_COUNT

__all__ = [
    "GATE_B_PLAN_SCHEMA_VERSION",
    "GATE_B_TARGET_SOURCE_RECORDS",
    "OPERAND_FIELDS",
    "Exp010GateBOperands",
    "Exp010GateBOperatorError",
    "MonotonicUtcClock",
    "build_gate_b_plan",
    "census_committed_source_count",
    "load_operands",
    "main",
    "run_gate_b_host_from_cli",
]


GATE_B_PLAN_SCHEMA_VERSION = "exp010-gate-b-plan-v1"
_PLAN_DOMAIN = b"VD::EXP010_GATE_B_PLAN::V1\x00"

#: The governed Gate-B source target: three complete 200-source windows.
GATE_B_TARGET_SOURCE_RECORDS = 3 * WINDOW_QUERY_COUNT

#: Read-only census consumer. `poll` never writes -- only `acknowledge` does --
#: and this id is never acknowledged with, so its cursor stays at sequence 0 and
#: a census always sees the true durable prefix regardless of what any real
#: consumer (for example Gate C's `v2-shadow`) has acknowledged.
GATE_B_CENSUS_CONSUMER_ID = "gate-b-census"

#: The canonical campaign-relative locations. Both are derived, never operands:
#: Gate A owns `gate_a/`, and these two are the rest of a campaign's layout.
STORE_SUBDIRECTORY = "stores"
OUTPUT_SUBDIRECTORY = "output"

#: Every store the Gate-B composition owns. Unlike Gate C, Gate B is the first
#: writer: absent stores are the correct fresh state, not a failure.
_COMPOSITION_STORES = (
    "v2_source.sqlite3",
    "v2_shadow_attempts.sqlite3",
    "v2_detector.sqlite3",
    "v2_attestation.sqlite3",
    "v2_window_finalization.sqlite3",
)

#: The exact, closed operand set. Anything else is refused, never defaulted.
#:
#: Deliberately tiny. Every governed identity is derived from verified Gate-A
#: evidence, so the only operands are the campaign to host, the one governed
#: value Gate A cannot supply (`detector_seed`), the loopback endpoint, the
#: container names the stack-health probe reads, and the source target.
OPERAND_FIELDS = (
    "campaign_root",
    "detector_seed",
    "host_address",
    "host_port",
    "target_source_records",
    "etcd_container",
    "minio_container",
)

#: Loopback only. A Gate-B host binds nothing an off-host process can reach.
_ALLOWED_HOST_ADDRESSES = frozenset({"127.0.0.1", "::1", "localhost"})

_ONE_MICROSECOND = timedelta(microseconds=1)


class Exp010GateBOperatorError(RuntimeError):
    """Fail-closed Gate-B operator error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010GateBOperatorError:
    return Exp010GateBOperatorError(code, message)


class MonotonicUtcClock:
    """Strictly increasing RFC3339 UTC timestamps for durable boundaries.

    Deliberately identical in behaviour to the Gate-C operator's clock: UTC is
    the *audit* timestamp, never causal authority. It exists only because the
    window assembler requires strictly increasing envelope timestamps; if the
    wall clock repeats or steps backward, the previous value is advanced by one
    microsecond rather than allowing a non-increasing durable record.
    """

    def __init__(self, now=None) -> None:
        self._now = now or (lambda: datetime.now(UTC))
        self._last: datetime | None = None

    def __call__(self) -> str:
        value = self._now()
        if value.tzinfo is None:
            raise _error("GATE_B_CLOCK_INVALID", "clock must be timezone-aware UTC")
        value = value.astimezone(UTC)
        if self._last is not None and value <= self._last:
            value = self._last + _ONE_MICROSECOND
        self._last = value
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class _RefusingShadowCaptureExecutor:
    """Any call is a bug: the Gate-B host never captures a shadow.

    This is the structural half of gate separation. Gate C's physical capture
    is unreachable from this entrypoint not because the code path is merely
    unused, but because the executor the composition would need raises.
    """

    def capture(self, sources: object, *, trace_sequence_index: int) -> Any:
        raise _error(
            "GATE_B_SHADOW_CAPTURE_FORBIDDEN",
            "the Gate-B host must never issue a physical shadow search",
        )


class _RefusingServingExecutor:
    """Any call is a bug: preflight serves nothing and issues zero searches."""

    def execute(self, request: object) -> Any:
        raise _error(
            "GATE_B_PREFLIGHT_SERVING_FORBIDDEN",
            "preflight must never serve a genuine request",
        )


@dataclass(frozen=True, slots=True)
class Exp010GateBOperands:
    """One fully validated Gate-B operand set plus its inherited authority."""

    campaign_root: Path
    detector_seed: int
    host_address: str
    host_port: int
    target_source_records: int
    etcd_container: str
    minio_container: str

    # -- inherited from verified Gate-A evidence, never operator-supplied ----
    authority: Mapping[str, Any]
    deployment_identity: str
    data_identity: str
    gate_a_evidence_sha256: str

    @property
    def store_root(self) -> Path:
        """Canonical, derived. Gate C reads `store_root.parent` symmetrically."""

        return self.campaign_root / STORE_SUBDIRECTORY

    @property
    def output_dir(self) -> Path:
        """Canonical, derived."""

        return self.campaign_root / OUTPUT_SUBDIRECTORY

    @property
    def metric(self) -> Metric:
        return Metric(str(self.authority["metric"]))

    def serving_configuration(self) -> Exp010ServingConfiguration:
        """Rebuild the exact serving semantics Gate A bound."""

        authority = self.authority
        return Exp010ServingConfiguration(
            metric=self.metric,
            threshold_stratum=str(authority["threshold_stratum"]),
            threshold_radius=float(authority["threshold_radius"]),
            range_filter=float(authority["range_filter"]),
            limit=int(authority["limit"]),
            served_ef=int(authority["served_ef"]),
            dimensions=int(authority["dimensions"]),
            consistency_level=str(authority["consistency_level"]),
        )

    def runner_configuration(self) -> Exp010OperatorConfiguration:
        """Compose the runner operand set from inherited authority."""

        authority = self.authority
        return Exp010OperatorConfiguration(
            milvus_uri=str(authority["milvus_uri"]),
            flat_collection_name=str(authority["flat_collection_name"]),
            hnsw_collection_name=str(authority["hnsw_collection_name"]),
            metric=self.metric,
            threshold_stratum=str(authority["threshold_stratum"]),
            threshold_radius=float(authority["threshold_radius"]),
            served_ef=int(authority["served_ef"]),
            detector_seed=self.detector_seed,
            stream_id=str(authority["stream_id"]),
            configuration_identity=str(authority["configuration_identity"]),
            flat_binding_id=str(authority["flat_binding_id"]),
            hnsw_binding_id=str(authority["hnsw_binding_id"]),
            source_revision=str(authority["source_revision"]),
            environment_manifest_sha256=str(authority["environment_manifest_sha256"]),
            store_root=self.store_root,
            dataset001_dir=Path(str(authority["dataset001_dir"])),
            exp010_output_dir=self.output_dir,
            consistency_level=str(authority["consistency_level"]),
        )


# --------------------------------------------------------------------------
# operand loading
# --------------------------------------------------------------------------


def _text(values: Mapping[str, Any], name: str) -> str:
    value = values[name]
    if not isinstance(value, str) or not value.strip():
        raise _error("GATE_B_OPERAND_INVALID", name)
    return value


def _exact_int(values: Mapping[str, Any], name: str) -> int:
    value = values[name]
    if isinstance(value, bool) or type(value) is not int:
        raise _error("GATE_B_OPERAND_INVALID", name)
    return value


def _positive_int(values: Mapping[str, Any], name: str) -> int:
    value = _exact_int(values, name)
    if value <= 0:
        raise _error("GATE_B_OPERAND_INVALID", name)
    return value


def load_operands(path: Path) -> Exp010GateBOperands:
    """Load, validate, and bind one exact-keyed Gate-B operand file.

    The key set is closed: a missing or unexpected key is refused rather than
    defaulted. Governed identities are then *inherited* from re-verified Gate-A
    evidence, so this function is the only place authority enters, and it can
    only enter from Gate A.
    """

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise _error("GATE_B_OPERANDS_UNREADABLE", str(exc)) from exc
    try:
        values = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("GATE_B_OPERANDS_MALFORMED", str(exc)) from exc
    if type(values) is not dict:
        raise _error("GATE_B_OPERANDS_MALFORMED", "operands must be a JSON object")

    missing = sorted(set(OPERAND_FIELDS) - set(values))
    if missing:
        raise _error("GATE_B_OPERANDS_INCOMPLETE", ",".join(missing))
    unexpected = sorted(set(values) - set(OPERAND_FIELDS))
    if unexpected:
        raise _error("GATE_B_OPERANDS_UNEXPECTED", ",".join(unexpected))

    campaign_root = Path(_text(values, "campaign_root"))
    host_address = _text(values, "host_address")
    if host_address not in _ALLOWED_HOST_ADDRESSES:
        raise _error(
            "GATE_B_HOST_ADDRESS_NOT_LOOPBACK",
            "the Gate-B host binds loopback only",
        )
    host_port = _exact_int(values, "host_port")
    if not 1 <= host_port <= 65535:
        raise _error("GATE_B_OPERAND_INVALID", "host_port")

    target = _positive_int(values, "target_source_records")
    if target % WINDOW_QUERY_COUNT != 0:
        raise _error(
            "GATE_B_TARGET_NOT_WHOLE_WINDOWS",
            f"target_source_records must be a multiple of {WINDOW_QUERY_COUNT}",
        )

    # `detector_seed` is a mandatory explicit operator decision. It is governed
    # by the detector-contract domain, deliberately excluded from
    # `configuration_identity`, and frozen per campaign. Gate A does not attest
    # it and must not be asked to: there is no default and no inference.
    detector_seed = _exact_int(values, "detector_seed")

    authority, deployment_identity, data_identity, evidence_digest = (
        _inherit_gate_a_authority(campaign_root)
    )

    return Exp010GateBOperands(
        campaign_root=campaign_root,
        detector_seed=detector_seed,
        host_address=host_address,
        host_port=host_port,
        target_source_records=target,
        etcd_container=_text(values, "etcd_container"),
        minio_container=_text(values, "minio_container"),
        authority=authority,
        deployment_identity=deployment_identity,
        data_identity=data_identity,
        gate_a_evidence_sha256=evidence_digest,
    )


def _inherit_gate_a_authority(
    campaign_root: Path,
) -> tuple[Mapping[str, Any], str, str, str]:
    """Re-verify Gate-A evidence and inherit every governed identity from it.

    Verification is mandatory and is never skipped because evidence is absent:
    missing, malformed, incomplete, substituted, or mismatched Gate-A evidence
    all fail closed. Nothing derivable from Gate A may be restated by an
    operator, so this is the only door authority comes through.
    """

    from .exp010_gate_a_operator import (
        Exp010GateAOperatorError,
        derive_downstream_authority,
        load_verified_gate_a_evidence,
    )

    try:
        evidence = load_verified_gate_a_evidence(campaign_root)
        authority = derive_downstream_authority(campaign_root)
    except Exp010GateAOperatorError as exc:
        raise _error(
            "GATE_B_GATE_A_AUTHORITY_UNVERIFIED", f"{campaign_root}: {exc.code}"
        ) from exc

    # The derived serving identity must be the one Gate A actually bound, or the
    # ingress would later admit requests under an identity the detector head
    # would refuse. Re-derive rather than trust.
    serving = Exp010ServingConfiguration(
        metric=Metric(str(authority["metric"])),
        threshold_stratum=str(authority["threshold_stratum"]),
        threshold_radius=float(authority["threshold_radius"]),
        range_filter=float(authority["range_filter"]),
        limit=int(authority["limit"]),
        served_ef=int(authority["served_ef"]),
        dimensions=int(authority["dimensions"]),
        consistency_level=str(authority["consistency_level"]),
    )
    if derive_serving_configuration_identity(serving) != str(
        authority["configuration_identity"]
    ):
        raise _error(
            "GATE_B_CONFIGURATION_AUTHORITY_MISMATCH",
            "Gate-A serving operands do not derive its configuration_identity",
        )

    return (
        authority,
        str(evidence["campaign"]["deployment_identity"]),
        str(evidence["dataset"]["data_identity"]),
        str(evidence["evidence_sha256"]),
    )


# --------------------------------------------------------------------------
# durable census
# --------------------------------------------------------------------------


def census_committed_source_count(composition: Any, *, ceiling: int) -> int:
    """Count durable source members using only the read-only committed API.

    `poll` reads; only `acknowledge` writes. The census consumer id is never
    acknowledged with, so its cursor stays at sequence 0 and the count is the
    true durable prefix no matter what a real consumer has acknowledged.
    """

    if type(ceiling) is not int or ceiling <= 0:
        raise _error("GATE_B_CENSUS_CEILING_INVALID")
    records = composition.response_store.poll(
        consumer_id=GATE_B_CENSUS_CONSUMER_ID,
        limit=ceiling + 1,
        start_source_sequence=0,
    )
    return len(records)


def _complete_window_count(composition: Any, *, ceiling: int) -> int:
    windows = 0
    while windows * WINDOW_QUERY_COUNT < ceiling:
        if composition.response_store.load_window(windows) is None:
            break
        windows += 1
    return windows


def _gate_c_state(composition: Any) -> dict[str, Any]:
    """Read the Gate-C-owned counters Gate B must leave at zero."""

    from .exp010_v2_host import SHADOW_CONSUMER_ID

    acknowledgements = composition.response_store.consumer_acknowledgement_state(
        consumer_id=SHADOW_CONSUMER_ID
    )
    return {
        "shadow_acknowledgements": len(acknowledgements.event_ids),
        "next_window_sequence": composition.finalization_store.next_window_sequence(),
    }


# --------------------------------------------------------------------------
# preflight plan
# --------------------------------------------------------------------------


def _endpoint_bindable(address: str, port: int) -> bool:
    """Prove the endpoint can be bound, then release it immediately.

    Binding and closing accepts no connection and creates no campaign state, so
    this stays inside preflight's side-effect-free contract.
    """

    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    probe = socket.socket(family, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((address, port))
    except OSError:
        return False
    finally:
        probe.close()
    return True


def _existing_stores(store_root: Path) -> tuple[str, ...]:
    if not store_root.is_dir():
        return ()
    return tuple(
        name for name in _COMPOSITION_STORES if (store_root / name).is_file()
    )


def build_gate_b_plan(operands: Exp010GateBOperands) -> dict[str, Any]:
    """Validate everything and return the resolved plan. Creates nothing.

    Preflight is side-effect free with respect to Gate-B campaign state: it
    never creates a store, never binds a listening server, and issues zero
    searches and zero `serve()` calls. Because Gate B is the *first* writer,
    absent stores are the correct fresh state; the composition is opened only
    when the stores already exist, so opening cannot bring a campaign into
    being as a side effect of inspecting it.
    """

    from .exp010_gate_a_operator import inspect_campaign_state

    campaign_state = str(inspect_campaign_state(operands.campaign_root))
    if campaign_state != "COMPLETE":
        raise _error("GATE_B_CAMPAIGN_NOT_COMPLETE", campaign_state)

    store_root = operands.store_root
    present = _existing_stores(store_root)
    if present and set(present) != set(_COMPOSITION_STORES):
        raise _error(
            "GATE_B_STORE_SET_INCOMPLETE",
            "a partially present store set is ambiguous and is never repaired",
        )
    restart = bool(present)

    durable_sources = 0
    complete_windows = 0
    gate_c_state: dict[str, Any] = {
        "shadow_acknowledgements": 0,
        "next_window_sequence": 0,
    }
    if restart:
        # Reopening an existing, complete store set verifies its durable
        # bindings against the inherited authority. It creates nothing.
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
            durable_sources = census_committed_source_count(
                composition, ceiling=operands.target_source_records
            )
            complete_windows = _complete_window_count(
                composition, ceiling=operands.target_source_records
            )
            gate_c_state = _gate_c_state(composition)
            if durable_sources > operands.target_source_records:
                raise _error(
                    "GATE_B_SOURCE_TARGET_EXCEEDED",
                    f"{durable_sources} durable sources exceed the governed target",
                )
        finally:
            runner.close()

    serving = operands.serving_configuration()
    authority = operands.authority

    # Probe once: a second probe would both waste a bind and race the first.
    bindable = _endpoint_bindable(operands.host_address, operands.host_port)
    if not bindable:
        raise _error(
            "GATE_B_ENDPOINT_UNBINDABLE",
            f"{operands.host_address}:{operands.host_port}",
        )

    plan: dict[str, Any] = {
        "schema_version": GATE_B_PLAN_SCHEMA_VERSION,
        "gate": "B",
        "canonical_operator": "vdbench.exp010_gate_b_operator",
        "canonical_ingress": "vdbench.exp010_ingress.Exp010RequestIngress.admit",
        "canonical_boundary": "vdbench.exp010_live_runner.Exp010LiveRunner.serve",
        "canonical_serving_executor": (
            "vdbench.milvus_serving.MilvusRangeServingExecutor"
        ),
        "serialization_contract": CANONICAL_JSON_SCHEMA_VERSION,
        "campaign": {
            "campaign_root": str(operands.campaign_root),
            "campaign_state": campaign_state,
            "deployment_identity": operands.deployment_identity,
            "store_root": str(store_root),
            "output_dir": str(operands.output_dir),
        },
        "gate_a": {
            "evidence_sha256": operands.gate_a_evidence_sha256,
            "environment_manifest_sha256": str(
                authority["environment_manifest_sha256"]
            ),
            "authority_inherited": True,
        },
        "stream": {
            "stream_id": str(authority["stream_id"]),
            "metric": str(authority["metric"]),
            "threshold_stratum": str(authority["threshold_stratum"]),
            "configuration_identity": str(authority["configuration_identity"]),
            "data_identity": operands.data_identity,
            "flat_binding_id": str(authority["flat_binding_id"]),
            "hnsw_binding_id": str(authority["hnsw_binding_id"]),
        },
        "serving": {
            "threshold_radius": serving.threshold_radius,
            "range_filter": serving.range_filter,
            "limit": serving.limit,
            "served_ef": serving.served_ef,
            "dimensions": serving.dimensions,
            "consistency_level": serving.consistency_level,
        },
        "detector_seed": operands.detector_seed,
        "endpoint": {
            "address": operands.host_address,
            "port": operands.host_port,
            "path": "/api/v1/search",
            "loopback_only": True,
            "bindable": bindable,
        },
        "source_target": {
            "target_source_records": operands.target_source_records,
            "window_size": WINDOW_QUERY_COUNT,
            "expected_windows": operands.target_source_records // WINDOW_QUERY_COUNT,
            "durable_source_records": durable_sources,
            "complete_windows": complete_windows,
            "remaining": operands.target_source_records - durable_sources,
        },
        "restart": {
            "stores_present": restart,
            "stores": list(_COMPOSITION_STORES),
            "state": "RESUME" if restart else "FRESH",
        },
        "gate_c": dict(gate_c_state),
        "source_revision": str(authority["source_revision"]),
        "physical_searches_issued_by_preflight": 0,
        "serve_calls_issued_by_preflight": 0,
        # Precisely what execute creates and nothing more: the composition
        # mkdirs `store_root` and opens the five stores. `output_dir` is Gate
        # E's capture location and is NOT created here, so claiming it would
        # overstate this gate's effects in what is audit evidence.
        "would_create": (
            []
            if restart
            else [str(store_root)]
            + [str(store_root / name) for name in _COMPOSITION_STORES]
        ),
        "would_not_create": {
            "gate_a_evidence": True,
            "gate_c_evidence": True,
            "output_dir": True,
        },
    }
    plan["plan_sha256"] = strict_canonical_digest(_PLAN_DOMAIN, plan)
    return plan


# --------------------------------------------------------------------------
# execute
# --------------------------------------------------------------------------


def run_gate_b_host_from_cli(operands: Exp010GateBOperands) -> dict[str, Any]:
    """The single real hosting seam.

    Reached only after `build_gate_b_plan` has validated everything, and only
    after the operator passed both `--mode execute` and the explicit
    confirmation flag. This repository never invokes it for real; tests patch
    this symbol to prove that preflight never reaches it.

    It constructs the canonical composition, binds loopback, and serves genuine
    external requests through the committed ingress until the governed source
    target is durable. It never generates a request, and it never advances a
    window.
    """

    from http.server import HTTPServer

    from .config import IndexTrack
    from .docker_health import DockerSocketHealthProbe
    from .exp010_ingress import Exp010RequestIngress, Exp010StdlibSearchHandler
    from .exp010_v2_host import pin_dataset001_identity
    from .milvus_actuation import CollectionIdentityBinding, MilvusHarness
    from .milvus_serving import HostServingPlan, MilvusRangeServingExecutor
    from .shadow_event_types import MonitorStreamKey
    from .v2_milvus_shadow_capture import build_readonly_milvus_client

    authority = operands.authority
    configuration = operands.runner_configuration()
    serving = operands.serving_configuration()

    client = build_readonly_milvus_client(str(authority["milvus_uri"]))
    stack_health_probe = DockerSocketHealthProbe(
        etcd_container=operands.etcd_container,
        minio_container=operands.minio_container,
    )
    harness = MilvusHarness(client, dimensions=serving.dimensions)
    flat_name = str(authority["flat_collection_name"])
    hnsw_name = str(authority["hnsw_collection_name"])
    dataset = pin_dataset001_identity(Path(str(authority["dataset001_dir"])))
    stream_key = MonitorStreamKey(
        configuration.stream_id,
        configuration.metric,
        configuration.threshold_stratum,
        configuration.configuration_identity,
        dataset.data_identity,
        configuration.flat_binding_id,
        configuration.hnsw_binding_id,
    )
    plan = HostServingPlan(
        flat_collection_name=flat_name,
        hnsw_collection_name=hnsw_name,
        flat_binding=CollectionIdentityBinding(
            identity_id=configuration.flat_binding_id,
            expected=harness.index_identity(
                flat_name, configuration.metric, IndexTrack.FLAT
            ),
        ),
        hnsw_binding=CollectionIdentityBinding(
            identity_id=configuration.hnsw_binding_id,
            expected=harness.index_identity(
                hnsw_name, configuration.metric, IndexTrack.HNSW
            ),
        ),
        threshold_radius=configuration.threshold_radius,
        dimensions=serving.dimensions,
        allowed_served_efs=frozenset({configuration.served_ef}),
    )
    serving_executor = MilvusRangeServingExecutor(
        client=client,
        plans={stream_key: plan},
        stack_health_probe=stack_health_probe,
    )
    # `ServingPreflightResult` exposes `complete`, `checked_stream_count` and
    # `reason_codes`. Read those fields directly: a defensive `getattr` with a
    # default silently turns a field-name mismatch into an unconditional
    # refusal instead of an error, which is exactly how the original defect
    # here refused a healthy stack.
    admission = serving_executor.preflight()
    if not admission.complete:
        raise _error(
            "GATE_B_SERVING_PREFLIGHT_REFUSED",
            ",".join(admission.reason_codes) or "NO_REASON_REPORTED",
        )

    clock = MonotonicUtcClock()
    runner = Exp010LiveRunner(
        configuration=configuration,
        serving_executor=serving_executor,
        # Gate B never captures a shadow; a capture call here is a defect, not
        # a fallback. This makes a Gate-C physical search structurally
        # impossible from the Gate-B host.
        shadow_capture_executor=_RefusingShadowCaptureExecutor(),
        clock=clock,
        shadow_captured_at_clock=clock,
    )
    try:
        composition = runner.composition
        target = operands.target_source_records
        ingress = Exp010RequestIngress(
            runner=runner, serving_configuration=serving
        )
        handler = Exp010StdlibSearchHandler.for_ingress(ingress)
        server = HTTPServer((operands.host_address, operands.host_port), handler)
        try:
            # One request at a time; the loop stops the instant the governed
            # target is durable. The host never manufactures a request, so it
            # simply waits for the external application.
            while census_committed_source_count(composition, ceiling=target) < target:
                server.handle_request()
        finally:
            server.server_close()

        durable = census_committed_source_count(composition, ceiling=target)
        windows = _complete_window_count(composition, ceiling=target)
        if durable != target:
            raise _error(
                "GATE_B_SOURCE_TARGET_NOT_MET", f"{durable} of {target} durable"
            )
        return {
            "schema_version": GATE_B_PLAN_SCHEMA_VERSION,
            "gate": "B",
            "durable_source_records": durable,
            "complete_windows": windows,
            "gate_c": _gate_c_state(composition),
        }
    finally:
        runner.close()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operands",
        type=Path,
        required=True,
        help="Path to the exact-keyed Gate-B operand JSON file.",
    )
    parser.add_argument(
        "--mode",
        choices=("preflight", "execute"),
        required=True,
        help=(
            "preflight validates and prints the plan, creating no store and "
            "accepting no request; execute additionally hosts genuine external "
            "ingress until the governed source target is durable."
        ),
    )
    parser.add_argument(
        "--confirm-gate-b-ingress",
        action="store_true",
        help=(
            "Required with --mode execute. A second, explicit operator action, "
            "deliberately separate from choosing the mode, acknowledging that a "
            "loopback server will accept genuine external requests and commit "
            "durable source membership."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Real operator entry point; never invoked by this repository's own code."""

    args = _parser().parse_args(argv)
    try:
        operands = load_operands(args.operands)
        plan = build_gate_b_plan(operands)
        if args.mode == "preflight":
            sys.stdout.write(
                strict_canonical_json_bytes(plan).decode("utf-8") + os.linesep
            )
            return 0
        if not args.confirm_gate_b_ingress:
            raise _error(
                "GATE_B_CONFIRMATION_REQUIRED",
                "--mode execute requires --confirm-gate-b-ingress",
            )
        result = run_gate_b_host_from_cli(operands)
        sys.stdout.write(
            strict_canonical_json_bytes(plan).decode("utf-8") + os.linesep
        )
        sys.stdout.write(
            strict_canonical_json_bytes(result).decode("utf-8") + os.linesep
        )
        return 0
    except Exp010GateBOperatorError as exc:
        sys.stderr.write(f"{exc.code}: {exc}{os.linesep}")
        return 2


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(main())
