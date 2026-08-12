"""Pure ADR-012 EXP-010 population projection from committed v2 observations.

The source adapter owns durable at-least-once delivery.  This module validates
the exact trigger N -> warm-up N+1 -> calibration N+2..N+7 relationship and
projects those already-served observations into existing R2-A immutable
population contracts.  It generates no query and creates no policy authority.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .config import IndexTrack, SearchConfiguration
from .host_window_detector_v2 import V2DetectorHead, verify_v2_detector_head
from .response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    WARMUP_QUERY_COUNT,
    CalibrationPopulationManifest,
    LiveStreamSourceNamespace,
    ResponseProfileRoleKind,
    ResponseProfileRoleManifest,
    build_calibration_population_manifest,
    build_canonical_query_identity,
    build_live_stream_source_namespace,
    build_query_vector_identity,
    build_response_profile_cell,
    build_response_profile_query_payload,
    build_response_profile_replay_schedule,
    build_response_profile_role,
    build_response_profile_role_manifest,
    build_response_profile_role_member,
    validate_role_manifest_disjointness,
)
from .response_profile_lifecycle import (
    ResponseProfileRunBinding,
    build_response_profile_run_binding,
)
from .response_profile_workload_capture import (
    GenuineWorkloadObservation,
    GenuineWorkloadObservationSource,
)

__all__ = [
    "V2CapturedPopulation",
    "V2PopulationCaptureError",
    "capture_v2_post_trigger_population",
]


class V2PopulationCaptureError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> V2PopulationCaptureError:
    return V2PopulationCaptureError(code)


@dataclass(frozen=True, slots=True)
class V2CapturedPopulation:
    trigger_window_sequence: int
    warmup_window_sequence: int
    calibration_window_sequences: tuple[int, ...]
    first_source_sequence: int
    last_source_sequence: int
    warmup_role_manifest: ResponseProfileRoleManifest
    population: CalibrationPopulationManifest
    run_binding: ResponseProfileRunBinding


def _member(
    observation: GenuineWorkloadObservation,
    *,
    namespace: LiveStreamSourceNamespace,
):
    vector = build_query_vector_identity(np.asarray(observation.query_vector, dtype="<f4"))
    configuration = SearchConfiguration(
        metric=observation.stream_key.metric,
        threshold_label=observation.stream_key.threshold_stratum,
        radius=observation.threshold_radius,
        index_track=IndexTrack.FLAT,
        ef=None,
        limit=observation.limit,
        consistency_level=observation.consistency_level,
    )
    return build_response_profile_role_member(
        source_namespace=namespace,
        query_identity=build_canonical_query_identity(observation.query_id),
        vector_identity=vector,
        query_payload_identity=build_response_profile_query_payload(
            vector_identity=vector, search_configuration=configuration
        ),
    )


def capture_v2_post_trigger_population(
    *,
    source: GenuineWorkloadObservationSource,
    trigger_head: V2DetectorHead,
    source_workload_manifest_sha256: str,
    run_id: str,
    created_at_utc: str,
    source_revision: str,
) -> V2CapturedPopulation:
    """Read, validate, acknowledge, and freeze exactly 1,400 v2 observations."""

    try:
        trigger_head = verify_v2_detector_head(trigger_head)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        raise _error("V2_TRIGGER_INVALID") from exc
    from .drift import DetectorState

    if trigger_head.detector_state is not DetectorState.DRIFT:
        raise _error("V2_TRIGGER_NOT_DRIFT")
    expected_count = WARMUP_QUERY_COUNT + CALIBRATION_QUERY_COUNT
    observations = source.poll(limit=expected_count)
    if type(observations) is not tuple or len(observations) != expected_count:
        raise _error("V2_CAPTURE_INCOMPLETE")
    first_sequence = (trigger_head.current_window_sequence + 1) * 200
    environment = observations[0].environment_manifest_sha256
    event_ids: set[str] = set()
    for offset, observation in enumerate(observations):
        expected_sequence = first_sequence + offset
        if (
            type(observation) is not GenuineWorkloadObservation
            or observation.source_sequence != expected_sequence
            or observation.window_sequence != expected_sequence // 200
            or observation.within_window_index != expected_sequence % 200
            or observation.stream_key != trigger_head.stream_key
            or observation.source_revision != source_revision
            or observation.environment_manifest_sha256 != environment
        ):
            raise _error("V2_CAPTURE_LINEAGE_INVALID")
        if observation.event_id in event_ids:
            raise _error("V2_CAPTURE_DUPLICATE")
        event_ids.add(observation.event_id)
    namespace = build_live_stream_source_namespace(
        stream_id=trigger_head.stream_key.stream_id,
        data_identity=trigger_head.stream_key.data_identity,
        source_workload_manifest_sha256=source_workload_manifest_sha256,
    )
    warmup_members = tuple(_member(item, namespace=namespace) for item in observations[:200])
    calibration_members = tuple(_member(item, namespace=namespace) for item in observations[200:])
    warmup = build_response_profile_role_manifest(
        role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_WARMUP),
        members=warmup_members,
    )
    calibration = build_response_profile_role_manifest(
        role=build_response_profile_role(kind=ResponseProfileRoleKind.RESPONSE_PROFILE_CALIBRATION),
        members=calibration_members,
    )
    validate_role_manifest_disjointness((warmup, calibration))
    population = build_calibration_population_manifest(
        cell=build_response_profile_cell(
            metric=trigger_head.stream_key.metric,
            threshold_stratum=trigger_head.stream_key.threshold_stratum,
        ),
        calibration_role_manifest=calibration,
    )
    schedule = build_response_profile_replay_schedule(
        population=population, source_revision=source_revision
    )
    run_binding = build_response_profile_run_binding(
        run_id=run_id, created_at_utc=created_at_utc, population=population,
        replay_schedule=schedule, warmup_role_manifest=warmup,
        source_revision=source_revision,
    )
    source.acknowledge(tuple(item.event_id for item in observations))
    return V2CapturedPopulation(
        trigger_window_sequence=trigger_head.current_window_sequence,
        warmup_window_sequence=trigger_head.current_window_sequence + 1,
        calibration_window_sequences=tuple(
            range(trigger_head.current_window_sequence + 2, trigger_head.current_window_sequence + 8)
        ),
        first_source_sequence=first_sequence,
        last_source_sequence=first_sequence + expected_count - 1,
        warmup_role_manifest=warmup,
        population=population,
        run_binding=run_binding,
    )
