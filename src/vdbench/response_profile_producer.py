"""Restart-safe offline EXP-010 response-profile producer composition.

The producer executes only through injected ports.  It commits each governed
MEASUREMENT_STARTED before invoking the query port and never derives or accepts
an independent raw-root pin.  Its successful output is the non-authorizing
R2-C semantic verification produced from an explicit verified ledger export.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
import time

from .config import SearchConfiguration
from .response_profile import ResponseProfileIdentity, SUPPORTED_EFS
from .response_profile_control import ResponseProfileControl, verify_response_profile_control
from .response_profile_evidence import ResponseProfileRoleMember
from .response_profile_lifecycle import LifecycleEventKind, ResponseProfileRunBinding
from .response_profile_lifecycle_ledger import ResponseProfileLifecycleLedger
from .response_profile_semantic import (
    MeasuredResultOutcome,
    ResponseProfileOracleManifest,
    ResponseProfileSemanticBundle,
    ResponseProfileSemanticEncoder,
    ResponseProfileSemanticExpectation,
    ResponseProfileSemanticVerification,
    ResponseProfileStaticIdentity,
    RuntimeSnapshotPhase,
    build_response_profile_identity_from_static,
    build_response_profile_semantic_encoder_from_static,
    oracle_manifest_document,
    verify_response_profile_semantic_bundle,
)


__all__ = [
    "ResponseProfileProducerError",
    "ResponseProfileExecutionQuery",
    "ResponseProfileSearchResult",
    "ResponseProfileRuntimeReadiness",
    "ResponseProfileQueryExecutor",
    "ResponseProfileRuntimeProbe",
    "ResponseProfileClock",
    "ResponseProfileProducerResult",
    "build_response_profile_search_result",
    "build_response_profile_runtime_readiness",
    "ResponseProfileProducer",
]


class ResponseProfileProducerError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> ResponseProfileProducerError:
    return ResponseProfileProducerError(message, code=code)


@dataclass(frozen=True, slots=True)
class ResponseProfileExecutionQuery:
    member: ResponseProfileRoleMember
    vector_bytes: bytes
    dimensions: int
    search_configuration: SearchConfiguration
    measured: bool


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileSearchResult:
    candidate_ids: tuple[int, ...]
    candidate_distances: tuple[float, ...]

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("search results must be built by the producer contract")


def build_response_profile_search_result(
    *, candidate_ids: tuple[int, ...], candidate_distances: tuple[float, ...]
) -> ResponseProfileSearchResult:
    if type(candidate_ids) is not tuple or type(candidate_distances) is not tuple:
        raise _error("SEARCH_RESULT_INVALID", "search result arrays must be tuples")
    if len(candidate_ids) != len(candidate_distances):
        raise _error("SEARCH_RESULT_INVALID", "search result arrays differ in length")
    if any(type(item) is not int or item < 0 for item in candidate_ids):
        raise _error("SEARCH_RESULT_INVALID", "candidate IDs must be non-negative integers")
    if len(set(candidate_ids)) != len(candidate_ids):
        raise _error("SEARCH_RESULT_INVALID", "candidate IDs must be distinct")
    if any(type(item) is not float or not (float("-inf") < item < float("inf")) for item in candidate_distances):
        raise _error("SEARCH_RESULT_INVALID", "candidate distances must be finite floats")
    value = object.__new__(ResponseProfileSearchResult)
    object.__setattr__(value, "candidate_ids", candidate_ids)
    object.__setattr__(value, "candidate_distances", candidate_distances)
    return value


@dataclass(frozen=True, slots=True, init=False)
class ResponseProfileRuntimeReadiness:
    collection_loaded: bool
    milvus_healthy: bool
    etcd_healthy: bool
    minio_healthy: bool

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("runtime readiness must be built by the producer contract")


def build_response_profile_runtime_readiness(
    *,
    collection_loaded: bool,
    milvus_healthy: bool,
    etcd_healthy: bool,
    minio_healthy: bool,
) -> ResponseProfileRuntimeReadiness:
    values = (collection_loaded, milvus_healthy, etcd_healthy, minio_healthy)
    if any(type(value) is not bool for value in values):
        raise _error("RUNTIME_READINESS_INVALID", "runtime readiness values must be bool")
    result = object.__new__(ResponseProfileRuntimeReadiness)
    for name, value in zip(
        ("collection_loaded", "milvus_healthy", "etcd_healthy", "minio_healthy"),
        values,
        strict=True,
    ):
        object.__setattr__(result, name, value)
    return result


class ResponseProfileQueryExecutor(Protocol):
    def execute(self, query: ResponseProfileExecutionQuery) -> ResponseProfileSearchResult: ...


class ResponseProfileRuntimeProbe(Protocol):
    def collect(self) -> ResponseProfileRuntimeReadiness: ...


class ResponseProfileClock(Protocol):
    def utc_now(self) -> str: ...
    def monotonic_ns(self) -> int: ...


class _SystemClock:
    def utc_now(self) -> str:
        # response_profile_lifecycle.py's strict RFC3339 validator accepts at
        # most 6 fractional digits; a 9-digit nanosecond fraction is rejected
        # outright, so this truncates to microsecond precision rather than
        # emitting a timestamp the ledger itself refuses.
        nanoseconds = time.time_ns()
        seconds, fraction_ns = divmod(nanoseconds, 1_000_000_000)
        microseconds = fraction_ns // 1_000
        base = datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        return f"{base}.{microseconds:06d}Z"

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()


@dataclass(frozen=True, slots=True)
class ResponseProfileProducerResult:
    complete: bool
    reason_codes: tuple[str, ...]
    closed_block_count: int
    completed_position_count: int
    warmup_search_calls: int
    measured_search_calls: int
    profile_identity: ResponseProfileIdentity | None
    semantic_verification: ResponseProfileSemanticVerification | None


class ResponseProfileProducer:
    """One restart-aware, block-bounded producer over an owned R2-B2 ledger."""

    def __init__(
        self,
        *,
        ledger: ResponseProfileLifecycleLedger,
        run_binding: ResponseProfileRunBinding,
        static_identity: ResponseProfileStaticIdentity,
        control: ResponseProfileControl,
        oracle_manifest: ResponseProfileOracleManifest,
        query_executor: ResponseProfileQueryExecutor,
        runtime_probe: ResponseProfileRuntimeProbe,
        clock: ResponseProfileClock | None = None,
    ) -> None:
        if type(ledger) is not ResponseProfileLifecycleLedger:
            raise _error("LEDGER_INVALID", "producer requires a concrete durable ledger")
        self._ledger = ledger
        self._binding = run_binding
        self._static_identity = static_identity
        try:
            self._control = verify_response_profile_control(control)
        except (AttributeError, TypeError, ValueError) as exc:
            raise _error("CONTROL_PROFILE_INVALID", "producer control is invalid") from exc
        self._oracle = oracle_manifest
        self._executor = query_executor
        self._probe = runtime_probe
        self._clock = _SystemClock() if clock is None else clock
        self._encoder: ResponseProfileSemanticEncoder = (
            build_response_profile_semantic_encoder_from_static(
                run_binding=run_binding, static_identity=static_identity
            )
        )
        if ledger.current_view().run_binding_sha256 != run_binding.run_binding_sha256:
            raise _error("LEDGER_BINDING_MISMATCH", "ledger run binding differs")
        if (
            self._control.control_profile_sha256 != static_identity.control_profile_sha256
            or self._control.calibration_population_sha256 != run_binding.workload_manifest_sha256
            or self._control.warmup_role_manifest_sha256 != run_binding.warmup_role_manifest_sha256
            or self._control.ordered_query_payload_sha256
            != run_binding.population.ordered_query_payload_sha256
            or self._control.replay_schedule_sha256 != run_binding.replay_schedule_sha256
            or self._control.environment_manifest_sha256
            != static_identity.environment_manifest_sha256
            or self._control.source_revision != static_identity.source_revision
            or self._control.stream_key.metric is not static_identity.metric
            or self._control.stream_key.threshold_stratum
            != static_identity.threshold_stratum
            or self._control.stream_key.data_identity != static_identity.data_identity
            or self._control.stream_key.hnsw_binding_id
            != static_identity.hnsw_index_identity
        ):
            raise _error("CONTROL_PROFILE_MISMATCH", "producer control differs from run identity")
        try:
            oracle_manifest_document(oracle_manifest)
        except (AttributeError, TypeError, ValueError) as exc:
            raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest is invalid") from exc
        if (
            oracle_manifest.workload_manifest_sha256
            != run_binding.workload_manifest_sha256
            or len(oracle_manifest.records) != len(run_binding.population.calibration_role_manifest.members)
        ):
            raise _error("ORACLE_MANIFEST_MISMATCH", "oracle manifest differs from population")

    @staticmethod
    def _query(
        member: ResponseProfileRoleMember,
        configuration: SearchConfiguration,
        *,
        measured: bool,
    ) -> ResponseProfileExecutionQuery:
        return ResponseProfileExecutionQuery(
            member=member,
            vector_bytes=bytes(member.vector_identity.canonical_vector_bytes),
            dimensions=member.vector_identity.dimensions,
            search_configuration=configuration,
            measured=measured,
        )

    def _execute(self, query: ResponseProfileExecutionQuery) -> ResponseProfileSearchResult:
        result = self._executor.execute(query)
        if type(result) is not ResponseProfileSearchResult:
            raise _error("SEARCH_RESULT_INVALID", "query executor returned a malformed result")
        return build_response_profile_search_result(
            candidate_ids=result.candidate_ids,
            candidate_distances=result.candidate_distances,
        )

    def _runtime_evidence(
        self, *, epoch: int, block: int, phase: RuntimeSnapshotPhase
    ) -> tuple[bytes, ResponseProfileRuntimeReadiness]:
        readiness = self._probe.collect()
        if type(readiness) is not ResponseProfileRuntimeReadiness:
            raise _error("RUNTIME_READINESS_INVALID", "runtime probe returned malformed evidence")
        evidence = self._encoder.runtime_snapshot(
            epoch_index=epoch,
            block_index=block,
            phase=phase,
            observed_at_utc=self._clock.utc_now(),
            collection_loaded=readiness.collection_loaded,
            milvus_healthy=readiness.milvus_healthy,
            etcd_healthy=readiness.etcd_healthy,
            minio_healthy=readiness.minio_healthy,
        )
        return evidence, readiness

    @staticmethod
    def _ready(readiness: ResponseProfileRuntimeReadiness) -> bool:
        return (
            readiness.collection_loaded
            and readiness.milvus_healthy
            and readiness.etcd_healthy
            and readiness.minio_healthy
        )

    def _start_fresh_epoch(self) -> int:
        view = self._ledger.current_view()
        epoch = 0 if not view.seen_epoch_indexes else max(view.seen_epoch_indexes) + 1
        self._ledger.begin_epoch(epoch_index=epoch, recorded_at_utc=self._clock.utc_now())
        configurations = {item.ef: item for item in self._encoder.configurations}
        for member in self._binding.warmup_role_manifest.members:
            for ef in SUPPORTED_EFS:
                self._execute(self._query(member, configurations[ef], measured=False))
        self._ledger.complete_warmup(
            evidence_bytes=self._encoder.warmup_execution(epoch_index=epoch),
            recorded_at_utc=self._clock.utc_now(),
        )
        return epoch

    def _finalize(self, *, warmup_calls: int, measured_calls: int) -> ResponseProfileProducerResult:
        exported = self._ledger.export_verified_lifecycle()
        started = next(
            event for event in exported.events
            if event.event_kind is LifecycleEventKind.MEASUREMENT_STARTED
        )
        completed = next(
            event for event in reversed(exported.events)
            if event.event_kind is LifecycleEventKind.MEASUREMENT_COMPLETED
        )
        identity = build_response_profile_identity_from_static(
            static_identity=self._static_identity,
            calibration_started_at_utc=started.recorded_at_utc,
            calibration_completed_at_utc=completed.recorded_at_utc,
            generated_at_utc=self._clock.utc_now(),
        )
        bundle = ResponseProfileSemanticBundle(
            calibration_population=self._binding.population,
            warmup_role_manifest=self._binding.warmup_role_manifest,
            replay_schedule=self._binding.replay_schedule,
            run_binding=exported.run_binding,
            events=exported.events,
            opaque_evidence=exported.opaque_evidence,
            oracle_manifest=self._oracle,
            control=self._control,
        )
        verification = verify_response_profile_semantic_bundle(
            bundle=bundle,
            expectation=ResponseProfileSemanticExpectation(
                profile_identity=identity,
                expected_oracle_manifest_sha256=self._oracle.oracle_manifest_sha256,
            ),
        )
        view = self._ledger.current_view()
        return ResponseProfileProducerResult(
            complete=True,
            reason_codes=(),
            closed_block_count=view.closed_block_count,
            completed_position_count=view.completed_position_count,
            warmup_search_calls=warmup_calls,
            measured_search_calls=measured_calls,
            profile_identity=identity,
            semantic_verification=verification,
        )

    def run(self, *, max_blocks: int | None = None) -> ResponseProfileProducerResult:
        if max_blocks is not None and (type(max_blocks) is not int or max_blocks <= 0):
            raise _error("MAX_BLOCKS_INVALID", "max_blocks must be a positive integer")
        view = self._ledger.current_view()
        if view.terminal_recovery:
            return ResponseProfileProducerResult(
                False, view.terminal_reason_codes, view.closed_block_count,
                view.completed_position_count, 0, 0, None, None
            )
        warmup_calls = 0
        measured_calls = 0
        if view.structurally_complete:
            return self._finalize(warmup_calls=0, measured_calls=0)
        if view.open_block_index is not None or view.open_measurement_position_index is not None:
            return ResponseProfileProducerResult(
                False, ("PARTIAL_MEASURED_BLOCK",), view.closed_block_count,
                view.completed_position_count, 0, 0, None, None
            )
        if view.requires_fresh_epoch_after_recovery or not view.warmup_completed_in_current_epoch:
            try:
                self._start_fresh_epoch()
                warmup_calls = len(self._binding.warmup_role_manifest.members) * len(SUPPORTED_EFS)
            except Exception:
                view = self._ledger.current_view()
                return ResponseProfileProducerResult(
                    False, ("WARMUP_EXECUTION_FAILED",), view.closed_block_count,
                    view.completed_position_count, warmup_calls, 0, None, None
                )

        remaining = len(self._binding.replay_schedule.blocks) - self._ledger.current_view().closed_block_count
        target = remaining if max_blocks is None else min(remaining, max_blocks)
        configurations = {item.ef: item for item in self._encoder.configurations}
        oracle_records = self._oracle.records
        members = self._binding.population.calibration_role_manifest.members
        for _ in range(target):
            view = self._ledger.current_view()
            block_index = view.closed_block_count
            epoch = view.current_epoch_index
            if epoch is None:
                raise _error("EPOCH_REQUIRED", "ledger has no active epoch")
            pre_bytes, pre_readiness = self._runtime_evidence(
                epoch=epoch,
                block=block_index,
                phase=RuntimeSnapshotPhase.PRE_BLOCK,
            )
            self._ledger.start_block(
                evidence_bytes=pre_bytes,
                recorded_at_utc=self._clock.utc_now(),
            )
            if not self._ready(pre_readiness):
                view = self._ledger.current_view()
                return ResponseProfileProducerResult(
                    False, ("RUNTIME_READINESS_FAILED",),
                    view.closed_block_count, view.completed_position_count,
                    warmup_calls, measured_calls, None, None
                )
            block = self._binding.replay_schedule.blocks[block_index]
            for position in block.positions:
                member = members[position.canonical_query_index]
                started_ns = self._clock.monotonic_ns()
                permit = self._ledger.start_measurement(
                    started_monotonic_ns=started_ns,
                    recorded_at_utc=self._clock.utc_now(),
                )
                measured_calls += 1
                outcome = MeasuredResultOutcome.SUCCESS
                failure_code: str | None = None
                ids: tuple[int, ...] = ()
                distances: tuple[float, ...] = ()
                try:
                    result = self._execute(
                        self._query(member, configurations[position.ef], measured=True)
                    )
                    ids, distances = result.candidate_ids, result.candidate_distances
                except TimeoutError:
                    outcome = MeasuredResultOutcome.TIMED_OUT
                    failure_code = "SEARCH_TIMED_OUT"
                except Exception:
                    outcome = MeasuredResultOutcome.FAILED
                    failure_code = "SEARCH_FAILED"
                completed_ns = self._clock.monotonic_ns()
                evidence = self._encoder.measured_result(
                    epoch_index=epoch,
                    block_index=block_index,
                    position_index=position.position_index,
                    measurement_started_event_sha256=permit.measurement_started_event_sha256,
                    observation_identity_sha256=position.observation_identity_sha256,
                    query_id_sha256=position.query_id_sha256,
                    query_payload_sha256=member.query_payload_identity.query_payload_sha256,
                    ef=position.ef,
                    oracle_record_sha256=oracle_records[position.canonical_query_index].oracle_record_sha256,
                    outcome=outcome,
                    candidate_ids=ids,
                    candidate_distances=distances,
                    failure_code=failure_code,
                )
                self._ledger.complete_measurement(
                    permit=permit,
                    evidence_bytes=evidence,
                    completed_monotonic_ns=completed_ns,
                    recorded_at_utc=self._clock.utc_now(),
                )
                if outcome is not MeasuredResultOutcome.SUCCESS:
                    view = self._ledger.current_view()
                    return ResponseProfileProducerResult(
                        False, (failure_code or "SEARCH_FAILED",),
                        view.closed_block_count, view.completed_position_count,
                        warmup_calls, measured_calls, None, None
                    )
            post_bytes, post_readiness = self._runtime_evidence(
                epoch=epoch,
                block=block_index,
                phase=RuntimeSnapshotPhase.POST_BLOCK,
            )
            self._ledger.close_block(
                evidence_bytes=post_bytes,
                recorded_at_utc=self._clock.utc_now(),
            )
            if not self._ready(post_readiness):
                view = self._ledger.current_view()
                return ResponseProfileProducerResult(
                    False, ("RUNTIME_READINESS_FAILED",),
                    view.closed_block_count, view.completed_position_count,
                    warmup_calls, measured_calls, None, None
                )

        view = self._ledger.current_view()
        if view.structurally_complete:
            return self._finalize(
                warmup_calls=warmup_calls, measured_calls=measured_calls
            )
        return ResponseProfileProducerResult(
            False, ("BOUNDED_PROGRESS",), view.closed_block_count,
            view.completed_position_count, warmup_calls, measured_calls, None, None
        )
