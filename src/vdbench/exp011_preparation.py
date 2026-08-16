"""Deterministic pre-run input preparation for EXP-011 acquisition.

This module closes the acquisition bootstrap without inventing evidence.  It
consumes frozen response-profile population/warm-up contracts, independently
constructed exact-oracle records, one store-verified latest detector head, and
explicit environment/index/source identities.  It produces the five immutable
documents that :mod:`vdbench.exp011_live_acquisition` requires *before* any
Milvus client or lifecycle ledger may be created.

The output is preparation evidence only.  It contains no search result,
latency, runtime epoch, lifecycle event, raw-evidence root, profile, freshness
capability, policy decision, grant, route, or actuation authority.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .artifacts import write_immutable_json
from .config import SearchConfiguration
from .exp011_live_acquisition import validate_exp011_governed_inputs
from .host_window_detector_v2 import (
    SQLiteHostWindowDetectorV2Store,
    VerifiedLatestV2DetectorHead,
)
from .response_profile_control import (
    ResponseProfileControl,
    build_response_profile_control,
    response_profile_control_document,
)
from .response_profile_evidence import (
    CalibrationPopulationManifest,
    ResponseProfileRoleManifest,
    build_response_profile_replay_schedule,
)
from .response_profile_lifecycle import (
    ResponseProfileRunBinding,
    build_response_profile_run_binding,
    response_profile_run_binding_document,
)
from .response_profile_monitor_store import ResponseProfileMonitorStateStore
from .response_profile_semantic import (
    ResponseProfileOracleManifest,
    ResponseProfileOracleRecord,
    ResponseProfileStaticIdentity,
    build_response_profile_oracle_manifest,
    build_response_profile_static_identity,
    oracle_manifest_document,
    response_profile_static_identity_document,
)
from .response_profile_vector_material import (
    load_response_profile_vector_material,
    response_profile_vector_material_document,
)
from .shadow_event_types import MonitorStreamKey

__all__ = [
    "Exp011PreparationError",
    "Exp011PreparedInputs",
    "prepare_exp011_acquisition_inputs",
]


class Exp011PreparationError(RuntimeError):
    """Fail-closed preparation error with a stable reason code."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def _error(code: str, message: str) -> Exp011PreparationError:
    return Exp011PreparationError(message, code=code)


@dataclass(frozen=True, slots=True)
class Exp011PreparedInputs:
    """The five published pre-run documents and their reconstructed values."""

    output_dir: Path
    run_binding_path: Path
    static_identity_path: Path
    control_path: Path
    oracle_manifest_path: Path
    vector_material_path: Path
    run_binding: ResponseProfileRunBinding
    static_identity: ResponseProfileStaticIdentity
    control: ResponseProfileControl
    oracle_manifest: ResponseProfileOracleManifest


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare_exp011_acquisition_inputs(
    *,
    output_dir: Path,
    monitor_store: ResponseProfileMonitorStateStore | SQLiteHostWindowDetectorV2Store,
    stream_key: MonitorStreamKey,
    population: CalibrationPopulationManifest,
    warmup_role_manifest: ResponseProfileRoleManifest,
    oracle_records: tuple[ResponseProfileOracleRecord, ...],
    run_id: str,
    created_at_utc: str,
    source_revision: str,
    search_configurations: tuple[SearchConfiguration, ...],
    hnsw_index_identity: str,
    data_identity: str,
    environment_manifest_sha256: str,
    frozen_at_utc: str,
) -> Exp011PreparedInputs:
    """Build and atomically publish the exact five EXP-011 pre-run inputs.

    ``population``, ``warmup_role_manifest`` and ``oracle_records`` are
    explicit external prerequisites.  In particular, this function never
    computes an oracle from acquisition responses and never creates detector
    evidence.  The latest detector head is read once from ``monitor_store``;
    its store-issued record identity is bound into the control document.
    """

    target = Path(output_dir)
    if target.exists():
        raise _error("PREPARATION_OUTPUT_EXISTS", "output directory already exists")
    if type(monitor_store) not in {
        ResponseProfileMonitorStateStore,
        SQLiteHostWindowDetectorV2Store,
    }:
        raise _error("PREPARATION_STORE_INVALID", "monitor store must be concrete")
    if type(stream_key) is not MonitorStreamKey:
        raise _error("PREPARATION_STREAM_INVALID", "stream key must be concrete")

    try:
        schedule = build_response_profile_replay_schedule(
            population=population,
            source_revision=source_revision,
        )
        run_binding = build_response_profile_run_binding(
            run_id=run_id,
            created_at_utc=created_at_utc,
            population=population,
            replay_schedule=schedule,
            warmup_role_manifest=warmup_role_manifest,
            source_revision=source_revision,
        )
        latest = monitor_store.load_verified_latest(stream_key)
        if latest is None:
            raise _error(
                "PREPARATION_DETECTOR_HEAD_REQUIRED",
                "a verified latest detector head is required",
            )
        if (
            type(monitor_store) is SQLiteHostWindowDetectorV2Store
            and type(latest) is not VerifiedLatestV2DetectorHead
        ):
            raise _error(
                "PREPARATION_DETECTOR_HEAD_INVALID",
                "v2 detector head must be store-issued",
            )
        head = latest.head
        control = build_response_profile_control(
            stream_key=stream_key,
            detector_provenance=head.detector_provenance,
            trigger_window_sequence=head.window_sequence,
            detector_head_sha256=head.detector_head_sha256,
            detector_head_record_sequence=latest.head_record_sequence,
            detector_head_record_sha256=latest.head_record_sha256,
            detector_head_persisted_at_utc=latest.head_record_persisted_at_utc,
            calibration_population_sha256=run_binding.workload_manifest_sha256,
            warmup_role_manifest_sha256=run_binding.warmup_role_manifest_sha256,
            ordered_query_payload_sha256=population.ordered_query_payload_sha256,
            replay_schedule_sha256=schedule.replay_schedule_sha256,
            environment_manifest_sha256=environment_manifest_sha256,
            source_revision=source_revision,
            frozen_at_utc=frozen_at_utc,
        )
        static_identity = build_response_profile_static_identity(
            metric=population.cell.metric,
            threshold_stratum=population.cell.threshold_stratum,
            search_configurations=search_configurations,
            hnsw_index_identity=hnsw_index_identity,
            data_identity=data_identity,
            workload_manifest_sha256=population.workload_manifest_sha256,
            ordered_query_payload_sha256=population.ordered_query_payload_sha256,
            replay_schedule_sha256=schedule.replay_schedule_sha256,
            control_profile_sha256=control.control_profile_sha256,
            environment_manifest_sha256=environment_manifest_sha256,
            source_revision=source_revision,
        )
        oracle_manifest = build_response_profile_oracle_manifest(
            population=population,
            records=oracle_records,
        )
        validate_exp011_governed_inputs(
            run_binding=run_binding,
            static_identity=static_identity,
            control=control,
            oracle_manifest=oracle_manifest,
        )
        documents = {
            "run_binding.json": response_profile_run_binding_document(run_binding),
            "static_identity.json": response_profile_static_identity_document(
                static_identity
            ),
            "control.json": response_profile_control_document(control),
            "oracle_manifest.json": oracle_manifest_document(oracle_manifest),
            "vector_material.json": response_profile_vector_material_document(
                run_binding
            ),
        }
        # Reconstruct the supplemental material before publication; the
        # acquisition loaders will independently repeat this verification.
        load_response_profile_vector_material(documents["vector_material.json"])
    except Exp011PreparationError:
        raise
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        raise _error("PREPARATION_INPUT_INVALID", "pre-run inputs are invalid") from exc

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        os.chmod(temporary, 0o700)
        for filename, document in documents.items():
            write_immutable_json(temporary / filename, document)
        _fsync_directory(temporary)
        if target.exists():
            raise _error(
                "PREPARATION_OUTPUT_EXISTS", "output directory appeared during publication"
            )
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise

    return Exp011PreparedInputs(
        output_dir=target,
        run_binding_path=target / "run_binding.json",
        static_identity_path=target / "static_identity.json",
        control_path=target / "control.json",
        oracle_manifest_path=target / "oracle_manifest.json",
        vector_material_path=target / "vector_material.json",
        run_binding=run_binding,
        static_identity=static_identity,
        control=control,
        oracle_manifest=oracle_manifest,
    )
