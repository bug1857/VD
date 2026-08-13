"""ADR-014 operator launcher for the real v2 host and EXP-010 progression.

Purpose:
    Compose the production v2 path and drive the deterministic windowing loop,
    with the five live gates kept explicitly separate. Starting this runner
    never captures EXP-010: reaching a real DRIFT only exposes a
    *trigger-ready* state, and the scientific capture (Gate E) requires a
    separate, explicit operator call.
Genuine workload:
    `serve(request)` IS the genuine workload ingress. This module generates no
    queries: no random vectors, no benchmark generator, no DATASET-002/003
    replay, no historical trace replay. A real application must call `serve`
    with its own `RangeQueryRequest` values, and nothing else may create source
    membership.
ADR-007 / ADR-013:
    `HostObservationRecorder.offer()` is neither imported nor called; the v2
    host is a sibling composition. A source-commit failure is never swallowed:
    it propagates out of `serve`, so a visible response always implies durable
    canonical membership.
Live boundary:
    Construction contacts nothing. The serving executor, shadow capture
    executor, and stack-health probe are injected, so the whole graph is
    constructible and testable offline with fakes.
Authority:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created or imported. B-001 is untouched.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any, Mapping

from .artifacts import canonical_json_bytes
from .config import Metric
from .drift import DetectorState
from .exp010_v2_host import Exp010V2HostComposition
from .host_observation import RangeQueryRequest
from .host_window_detector_v2 import HostWindowV2Status
from .host_window_lineage import V2GenuineWorkloadObservationSource, V2VisibleResponse
from .monitor_evidence import encode_persisted_window_evidence
from .real_detector_attestation_store import VerifiedRealDetectorHead
from .response_profile_v2_capture import capture_real_v2_post_trigger_population
from .shadow_window import WINDOW_QUERY_COUNT


__all__ = [
    "ENVIRONMENT_IDENTITY_SCHEMA_VERSION",
    "Exp010LiveRunnerError",
    "Exp010OperatorConfiguration",
    "Exp010WindowResult",
    "Exp010TriggerState",
    "Exp010LiveRunner",
    "build_environment_manifest_sha256",
]


ENVIRONMENT_IDENTITY_SCHEMA_VERSION = "v2-host-environment-identity-v1"
_ENVIRONMENT_DOMAIN = b"VD::V2_HOST_ENVIRONMENT_IDENTITY::V1\x00"

_ENVIRONMENT_FIELDS = (
    "milvus_uri",
    "deployment_identity",
    "flat_collection_name",
    "hnsw_collection_name",
    "metric",
    "threshold_stratum",
    "dimensions",
    "flat_index_identity",
    "hnsw_index_identity",
    "data_identity",
    "source_revision",
    "served_ef",
    "observed_at_utc",
)


class Exp010LiveRunnerError(RuntimeError):
    """Fail-closed runner error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010LiveRunnerError:
    return Exp010LiveRunnerError(code, message)


def build_environment_manifest_sha256(observed: Mapping[str, object]) -> str:
    """Canonicalize already-observed Gate-A metadata into one environment digest.

    Deterministic and offline: this helper hashes values an operator has
    *already* read from the live stack; it contacts nothing itself. Every field
    is mandatory, so a historical digest can never be silently reused in place
    of a fresh Gate-A observation -- a stale `observed_at_utc` or a changed
    index identity yields a different digest.
    """

    if not isinstance(observed, Mapping):
        raise _error("ENVIRONMENT_METADATA_INVALID")
    missing = [name for name in _ENVIRONMENT_FIELDS if name not in observed]
    if missing:
        raise _error("ENVIRONMENT_METADATA_INCOMPLETE", ",".join(sorted(missing)))
    unexpected = sorted(set(observed) - set(_ENVIRONMENT_FIELDS))
    if unexpected:
        raise _error("ENVIRONMENT_METADATA_UNEXPECTED", ",".join(unexpected))
    payload: dict[str, object] = {"schema_version": ENVIRONMENT_IDENTITY_SCHEMA_VERSION}
    for name in _ENVIRONMENT_FIELDS:
        value = observed[name]
        if isinstance(value, Metric):
            value = value.value
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise _error("ENVIRONMENT_METADATA_INVALID", name)
        if isinstance(value, str) and not value:
            raise _error("ENVIRONMENT_METADATA_INVALID", name)
        payload[name] = value
    return hashlib.sha256(
        _ENVIRONMENT_DOMAIN + canonical_json_bytes(payload)
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class Exp010OperatorConfiguration:
    """The strict operand set an operator supplies for a real run.

    Deliberately absent, because each is derived or store-issued rather than
    asserted: `data_identity` (derived from the verified DATASET-001 corpus),
    the detector-contract digest (derived from `drift.py` constants), the
    real-detector attestation (evaluator-issued), and any DRIFT flag (only a
    governed evaluation can produce one). The environment digest must come from
    `build_environment_manifest_sha256` over verified Gate-A metadata.
    """

    milvus_uri: str
    flat_collection_name: str
    hnsw_collection_name: str
    metric: Metric
    threshold_stratum: str
    threshold_radius: float
    served_ef: int
    detector_seed: int
    stream_id: str
    configuration_identity: str
    flat_binding_id: str
    hnsw_binding_id: str
    source_revision: str
    environment_manifest_sha256: str
    store_root: Path
    dataset001_dir: Path
    exp010_output_dir: Path
    consistency_level: str = "Strong"

    def __post_init__(self) -> None:
        if type(self.metric) is not Metric:
            raise _error("CONFIG_METRIC_INVALID")
        for name in (
            "milvus_uri", "flat_collection_name", "hnsw_collection_name",
            "threshold_stratum", "stream_id", "configuration_identity",
            "flat_binding_id", "hnsw_binding_id", "source_revision",
            "consistency_level",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise _error("CONFIG_FIELD_INVALID", name)
        if (
            not isinstance(self.environment_manifest_sha256, str)
            or len(self.environment_manifest_sha256) != 64
        ):
            raise _error("CONFIG_ENVIRONMENT_INVALID")
        if isinstance(self.served_ef, bool) or not isinstance(self.served_ef, int):
            raise _error("CONFIG_SERVED_EF_INVALID")
        if isinstance(self.detector_seed, bool) or not isinstance(self.detector_seed, int):
            raise _error("CONFIG_DETECTOR_SEED_INVALID")


@dataclass(frozen=True, slots=True)
class Exp010WindowResult:
    """Outcome of processing exactly one canonical 200-source window."""

    window_sequence: int
    status: HostWindowV2Status
    reason_codes: tuple[str, ...]
    detector_state: DetectorState | None
    attested: bool


@dataclass(frozen=True, slots=True)
class Exp010TriggerState:
    """Whether a governed real DRIFT trigger is currently available.

    `trigger_ready` never starts a capture. Gate E is a separate explicit
    operator call to `capture_exp010_population`.
    """

    trigger_ready: bool
    reason: str
    verified_real_head: VerifiedRealDetectorHead | None
    trigger_window_sequence: int | None
    exp010_start_source_sequence: int | None


class Exp010LiveRunner:
    """Operator launcher: genuine ingress, windowing loop, and gate separation."""

    def __init__(
        self,
        *,
        configuration: Exp010OperatorConfiguration,
        serving_executor: Any,
        shadow_capture_executor: Any,
        clock: Callable[[], str],
        shadow_captured_at_clock: Callable[[], str],
    ) -> None:
        if type(configuration) is not Exp010OperatorConfiguration:
            raise _error("CONFIG_INVALID")
        self.configuration = configuration
        self.composition = Exp010V2HostComposition(
            root=configuration.store_root,
            dataset001_dir=configuration.dataset001_dir,
            stream_id=configuration.stream_id,
            metric=configuration.metric,
            threshold_stratum=configuration.threshold_stratum,
            configuration_identity=configuration.configuration_identity,
            flat_binding_id=configuration.flat_binding_id,
            hnsw_binding_id=configuration.hnsw_binding_id,
            source_revision=configuration.source_revision,
            environment_manifest_sha256=configuration.environment_manifest_sha256,
            serving_executor=serving_executor,
            shadow_capture_executor=shadow_capture_executor,
            detector_seed=configuration.detector_seed,
            clock=clock,
            shadow_captured_at_clock=shadow_captured_at_clock,
        )
        self._clock = clock
        self._shadow_cursor = 0
        self._closed = False

    # -- Gate B: genuine workload ingress --------------------------------

    def serve(self, request: RangeQueryRequest) -> V2VisibleResponse:
        """THE genuine workload boundary. A real application calls this.

        This runner never manufactures a request. A durable source-commit
        failure propagates (ADR-013): no visible response is returned unless
        canonical membership is committed first.
        """

        if type(request) is not RangeQueryRequest:
            raise _error("REQUEST_INVALID")
        return self.composition.execute(request)

    # -- Gate C/D: deterministic windowing loop --------------------------

    def process_ready_windows(self) -> tuple[Exp010WindowResult, ...]:
        """Process every *complete* canonical 200-source window now available.

        Only whole windows are processed, in order, from the independent
        `v2-shadow` cursor; an incomplete tail stays pending and is never
        acknowledged. Restart is safe because the cursor is durable and the
        detector store reconstructs its own progression.
        """

        results: list[Exp010WindowResult] = []
        while True:
            observations = self.composition.shadow_source.poll(
                limit=WINDOW_QUERY_COUNT
            )
            if len(observations) < WINDOW_QUERY_COUNT:
                break  # incomplete tail: remains pending, not acknowledged
            sources = self.composition.response_store.poll(
                consumer_id="v2-shadow-sources",
                limit=WINDOW_QUERY_COUNT,
                start_source_sequence=self._shadow_cursor,
            )
            if len(sources) != WINDOW_QUERY_COUNT:
                break
            results.append(self._process_window(tuple(sources)))
            self.composition.shadow_source.acknowledge(
                tuple(item.event_id for item in observations)
            )
            self._shadow_cursor += WINDOW_QUERY_COUNT
        return tuple(results)

    def _process_window(self, sources) -> Exp010WindowResult:
        bundle = self.composition.shadow_worker.build(sources)
        reference_bundle = getattr(self, "_reference_bundle", None)
        captured: dict[str, object] = {}

        def evaluator(reference_window, current_window):
            if reference_bundle is None:
                raise _error("WINDOW_REFERENCE_MISSING")
            decision, pending = self.composition.evaluator.evaluate(
                reference_shadow_window=reference_bundle.shadow_window,
                current_shadow_window=bundle.shadow_window,
                reference_assembled=reference_bundle.assembled,
                current_assembled=bundle.assembled,
                reference_sources=reference_bundle.sources,
                current_sources=bundle.sources,
                metric=self.configuration.metric,
            )
            captured["pending"] = pending
            return decision

        result = self.composition.detector_store.process_window(
            window=bundle.shadow_window,
            evaluator=evaluator,
            persisted_at_utc=self._clock(),
        )
        attested = False
        if result.detector_head is not None and "pending" in captured:
            pending = captured["pending"]
            evidence = pending["current_window_evidence"]
            encoded = encode_persisted_window_evidence(evidence)
            attestation = self.composition.evaluator.attest(
                pending=pending,
                head=result.detector_head,
                current_window_evidence_sha256=encoded["sha256"],
            )
            self.composition.attestation_store.append(
                attestation=attestation, window_evidence=evidence
            )
            attested = True
        # REBASELINE (and the first window) establishes the reference epoch;
        # UNEVALUABLE deliberately leaves the reference untouched.
        if result.status is HostWindowV2Status.REBASELINE:
            self._reference_bundle = bundle
        return Exp010WindowResult(
            window_sequence=bundle.window_sequence,
            status=result.status,
            reason_codes=result.reason_codes,
            detector_state=(
                None if result.detector_head is None
                else result.detector_head.detector_state
            ),
            attested=attested,
        )

    # -- Gate D: trigger readiness (never auto-captures) -----------------

    def trigger_state(self) -> Exp010TriggerState:
        """Report whether a governed real DRIFT trigger exists. Captures nothing."""

        real = self.composition.verified_real_latest()
        if real is None:
            return Exp010TriggerState(
                False, "NO_VERIFIED_REAL_HEAD", None, None, None
            )
        if real.head.detector_state is not DetectorState.DRIFT:
            return Exp010TriggerState(
                False,
                f"REAL_HEAD_NOT_DRIFT:{real.head.detector_state.value}",
                real, real.head.current_window_sequence, None,
            )
        trigger = real.head.current_window_sequence
        return Exp010TriggerState(
            True, "REAL_DRIFT_TRIGGER_READY", real, trigger,
            (trigger + 1) * WINDOW_QUERY_COUNT,
        )

    # -- Gate E: explicit operator action only ---------------------------

    def capture_exp010_population(
        self, *, run_id: str, source_workload_manifest_sha256: str
    ):
        """GATE E. Explicit operator action; never invoked by the loop.

        Refuses unless a `VerifiedRealDetectorHead` in state DRIFT exists.
        """

        state = self.trigger_state()
        if not state.trigger_ready or state.verified_real_head is None:
            raise _error("EXP010_TRIGGER_NOT_READY", state.reason)
        source = self.composition.exp010_source(
            start_source_sequence=state.exp010_start_source_sequence
        )
        return capture_real_v2_post_trigger_population(
            source=source,
            verified_real_head=state.verified_real_head,
            source_workload_manifest_sha256=source_workload_manifest_sha256,
            run_id=run_id,
            created_at_utc=self._clock(),
            source_revision=self.configuration.source_revision,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.composition.close()

    def __enter__(self) -> "Exp010LiveRunner":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
