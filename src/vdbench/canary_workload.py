"""Immutable workload and selection evidence for EXP-009 Stage 1.

This module freezes the finite 600-occurrence workload before a CSPRNG selects
the exact 60 candidate occurrence IDs.  It deliberately contains no routing,
Milvus, policy, approval, or actuation dependency.  A selection record binds
the persisted workload *file* digest, never raw CSPRNG entropy or query vectors.

The record proves artifact integrity and that this code used ``SystemRandom``;
it does not by itself prove a future live run's no-interference assumption.
That remains a separately captured Stage-4 condition under ADR-008.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import tempfile
from typing import Any

import numpy as np

from .artifacts import canonical_json_bytes, sha256_file
from .canary_statistics import (
    EXP009_CANDIDATE_COUNT,
    EXP009_ROUTING_POPULATION_COUNT,
)
from .config import ContractViolation, HNSW_EF_SWEEP, Metric, RESULT_LIMIT, THRESHOLD_LABELS
from .dataset002 import verify_dataset002_artifacts


ELIGIBLE_WORKLOAD_SCHEMA_VERSION = "exp009-eligible-workload-manifest-v2"
CANDIDATE_SELECTION_SCHEMA_VERSION = "exp009-candidate-selection-record-v1"
SCHEDULE_STABILITY_SCHEMA_VERSION = "exp009-schedule-stability-v1"
VECTOR_MAPPING_ONE_TO_ONE = "one_to_one_unique_dataset002_routing_vectors"
SYSTEM_RANDOM_SOURCE = "python.secrets.SystemRandom.sample"
SCHEDULE_CONTROL_ROLE = "recall_audit"
SCHEDULE_CONTROL_COUNT = 50
SCHEDULE_PRE_SWEEP_COUNT = 3
SCHEDULE_ROUTING_BLOCK_SIZE = 100
SCHEDULE_INTERLEAVED_SWEEP_COUNT = 6
SCHEDULE_POST_SWEEP_COUNT = 3
SCHEDULE_EXECUTION_MODE = "synchronous_serial_manifest_order"
SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING = 10.0
SCHEDULE_P95_RELATIVE_CEILING = 1.50
SCHEDULE_MEDIAN_RELATIVE_CEILING = 1.25
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_OCCURRENCE_RE = re.compile(r"exp009-routing-([0-9]{6})\Z")
_UTC_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z")
_ACTUATION_LADDER = tuple(value for value in HNSW_EF_SWEEP if value != 100)


class _DuplicateJsonField(ValueError):
    """Raised only while rejecting duplicate fields in an evidence document."""


def _no_duplicate_json_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonField(key)
        result[key] = value
    return result


def _utc_timestamp(value: object, *, field: str) -> str:
    """Validate an RFC3339 UTC instant, including its calendar values."""

    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractViolation(f"{field} has invalid calendar values") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ContractViolation(f"{field} must use UTC Z")
    return value


def _timestamp_instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be a lower-case SHA-256")
    return value


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ContractViolation(f"{field} must be a non-empty canonical string")
    return value


def _integer(value: object, *, field: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractViolation(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise ContractViolation(f"{field} is below its minimum")
    return value


def _finite_float(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(f"{field} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ContractViolation(f"{field} must be a finite number")
    return normalized


def _exact_mapping(value: object, fields: frozenset[str], *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or frozenset(value) != fields:
        raise ContractViolation(f"{field} has an invalid schema")
    return value


def _metric(value: object, *, field: str) -> Metric:
    try:
        return Metric(value)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{field} is not an authorized metric") from exc


def _validate_transition(*, candidate_ef: object, last_known_good_ef: object) -> tuple[int, int]:
    candidate = _integer(candidate_ef, field="candidate_ef", minimum=1)
    lkg = _integer(last_known_good_ef, field="last_known_good_ef", minimum=1)
    if candidate not in _ACTUATION_LADDER or lkg not in _ACTUATION_LADDER:
        raise ContractViolation("candidate_ef and last_known_good_ef must use the actuation ladder")
    if abs(_ACTUATION_LADDER.index(candidate) - _ACTUATION_LADDER.index(lkg)) != 1:
        raise ContractViolation("candidate_ef and last_known_good_ef must be adjacent")
    return candidate, lkg


def _expected_data_identity(inherited: Mapping[str, object]) -> str:
    _text(inherited.get("dataset_id"), field="inherited_dataset001.dataset_id")
    version = _text(inherited.get("version"), field="inherited_dataset001.version")
    manifest_sha = _sha256(
        inherited.get("generation_manifest_sha256"),
        field="inherited_dataset001.generation_manifest_sha256",
    )
    # EXP-005's verified identity convention prefixes the persisted version
    # (``DATASET-001-v1``), not a duplicated dataset-id/version pair.
    return f"{version}:sha256:{manifest_sha}"


@dataclass(frozen=True, slots=True)
class WorkloadIdentityBinding:
    """Opaque data/configuration/index identities bound to one workload."""

    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str

    def validate(self) -> None:
        for field, value in (
            ("configuration_identity", self.configuration_identity),
            ("data_identity", self.data_identity),
            ("flat_binding_id", self.flat_binding_id),
            ("hnsw_binding_id", self.hnsw_binding_id),
        ):
            _text(value, field=field)

    def to_document(self) -> dict[str, str]:
        self.validate()
        return {
            "configuration_identity": self.configuration_identity,
            "data_identity": self.data_identity,
            "flat_binding_id": self.flat_binding_id,
            "hnsw_binding_id": self.hnsw_binding_id,
        }


@dataclass(frozen=True, slots=True)
class EligibleOccurrence:
    """One non-sensitive binding to a DATASET-002 routing vector."""

    sequence_index: int
    occurrence_id: str
    dataset_query_id: int
    vector_sha256: str
    threshold_radius: float
    range_filter: float
    limit: int

    def validate(self) -> None:
        sequence = _integer(self.sequence_index, field="occurrence.sequence_index", minimum=0)
        query_id = _integer(self.dataset_query_id, field="occurrence.dataset_query_id", minimum=0)
        occurrence_id = _text(self.occurrence_id, field="occurrence.occurrence_id")
        expected = f"exp009-routing-{query_id:06d}"
        if occurrence_id != expected or _OCCURRENCE_RE.fullmatch(occurrence_id) is None:
            raise ContractViolation("occurrence ID is not canonical")
        if sequence != query_id:
            raise ContractViolation("occurrence sequence/query binding is invalid")
        _sha256(self.vector_sha256, field="occurrence.vector_sha256")
        _finite_float(self.threshold_radius, field="occurrence.threshold_radius")
        _finite_float(self.range_filter, field="occurrence.range_filter")
        if _integer(self.limit, field="occurrence.limit", minimum=1) != RESULT_LIMIT:
            raise ContractViolation("occurrence.limit must equal the frozen result limit")

    def to_document(self) -> dict[str, object]:
        self.validate()
        return {
            "sequence_index": self.sequence_index,
            "occurrence_id": self.occurrence_id,
            "dataset_query_id": self.dataset_query_id,
            "vector_sha256": self.vector_sha256,
            "threshold_radius": self.threshold_radius,
            "range_filter": self.range_filter,
            "limit": self.limit,
        }


@dataclass(frozen=True, slots=True)
class ScheduleControl:
    """One vector binding for the pre-registered LKG-only control sweeps.

    It carries no raw vector.  The ID and digest are rebuilt from DATASET-002
    before any Stage-4 run can use the schedule contract.
    """

    query_id: int
    vector_sha256: str

    def validate(self) -> None:
        _integer(self.query_id, field="schedule control query_id", minimum=0)
        _sha256(self.vector_sha256, field="schedule control vector_sha256")

    def to_document(self) -> dict[str, object]:
        self.validate()
        return {"query_id": self.query_id, "vector_sha256": self.vector_sha256}


@dataclass(frozen=True, slots=True)
class ScheduleStabilityContract:
    """Immutable falsification protocol for the Stage-4 SUTVA diagnostic.

    The contract deliberately defines observable environment-stability checks;
    it does not claim that passing them proves a no-interference assumption.
    """

    schema_version: str
    control_role: str
    control_ef: int
    controls: tuple[ScheduleControl, ...]
    pre_sweep_count: int
    routing_block_size: int
    interleaved_sweep_count: int
    post_sweep_count: int
    execution_mode: str
    absolute_p95_latency_ms_ceiling: float
    p95_relative_ceiling: float
    median_relative_ceiling: float
    require_all_success: bool
    require_identity_and_health_per_sweep: bool

    @property
    def control_query_ids(self) -> tuple[int, ...]:
        return tuple(control.query_id for control in self.controls)

    @property
    def control_vector_sha256(self) -> tuple[str, ...]:
        return tuple(control.vector_sha256 for control in self.controls)

    def validate(self) -> None:
        if self.schema_version != SCHEDULE_STABILITY_SCHEMA_VERSION:
            raise ContractViolation("schedule stability schema version is invalid")
        if self.control_role != SCHEDULE_CONTROL_ROLE:
            raise ContractViolation("schedule stability control role is invalid")
        if _integer(self.control_ef, field="schedule stability control_ef", minimum=1) not in _ACTUATION_LADDER:
            raise ContractViolation("schedule stability control_ef is not on the actuation ladder")
        if len(self.controls) != SCHEDULE_CONTROL_COUNT:
            raise ContractViolation("schedule stability controls must contain exactly 50 entries")
        expected_ids = tuple(
            range(
                EXP009_ROUTING_POPULATION_COUNT,
                EXP009_ROUTING_POPULATION_COUNT + SCHEDULE_CONTROL_COUNT,
            )
        )
        if self.control_query_ids != expected_ids:
            raise ContractViolation("schedule stability controls must use frozen recall-audit IDs")
        if len(set(self.control_vector_sha256)) != SCHEDULE_CONTROL_COUNT:
            raise ContractViolation("schedule stability controls must bind unique vectors")
        for control in self.controls:
            control.validate()
        if self.pre_sweep_count != SCHEDULE_PRE_SWEEP_COUNT:
            raise ContractViolation("schedule stability pre-sweep count is invalid")
        if self.routing_block_size != SCHEDULE_ROUTING_BLOCK_SIZE:
            raise ContractViolation("schedule stability routing block size is invalid")
        if self.interleaved_sweep_count != SCHEDULE_INTERLEAVED_SWEEP_COUNT:
            raise ContractViolation("schedule stability interleaved sweep count is invalid")
        if self.post_sweep_count != SCHEDULE_POST_SWEEP_COUNT:
            raise ContractViolation("schedule stability post-sweep count is invalid")
        if self.execution_mode != SCHEDULE_EXECUTION_MODE:
            raise ContractViolation("schedule stability execution mode is invalid")
        if (
            _finite_float(
                self.absolute_p95_latency_ms_ceiling,
                field="schedule stability absolute_p95_latency_ms_ceiling",
            )
            != SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING
        ):
            raise ContractViolation("schedule stability absolute p95 ceiling is invalid")
        if (
            _finite_float(
                self.p95_relative_ceiling,
                field="schedule stability p95_relative_ceiling",
            )
            != SCHEDULE_P95_RELATIVE_CEILING
        ):
            raise ContractViolation("schedule stability relative p95 ceiling is invalid")
        if (
            _finite_float(
                self.median_relative_ceiling,
                field="schedule stability median_relative_ceiling",
            )
            != SCHEDULE_MEDIAN_RELATIVE_CEILING
        ):
            raise ContractViolation("schedule stability relative median ceiling is invalid")
        if self.require_all_success is not True:
            raise ContractViolation("schedule stability must require all control responses")
        if self.require_identity_and_health_per_sweep is not True:
            raise ContractViolation("schedule stability must require identity and health per sweep")

    def to_document(self) -> dict[str, object]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "control_role": self.control_role,
            "control_ef": self.control_ef,
            "controls": [control.to_document() for control in self.controls],
            "pre_sweep_count": self.pre_sweep_count,
            "routing_block_size": self.routing_block_size,
            "interleaved_sweep_count": self.interleaved_sweep_count,
            "post_sweep_count": self.post_sweep_count,
            "execution_mode": self.execution_mode,
            "absolute_p95_latency_ms_ceiling": self.absolute_p95_latency_ms_ceiling,
            "p95_relative_ceiling": self.p95_relative_ceiling,
            "median_relative_ceiling": self.median_relative_ceiling,
            "require_all_success": self.require_all_success,
            "require_identity_and_health_per_sweep": self.require_identity_and_health_per_sweep,
        }


@dataclass(frozen=True, slots=True)
class EligibleWorkloadManifest:
    """Canonical, immutable input population for one EXP-009 transition."""

    schema_version: str
    created_at_utc: str
    dataset002_manifest_sha256: str
    dataset001_generation_manifest_sha256: str
    metric: Metric
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    radius: float
    range_filter: float
    limit: int
    identity: WorkloadIdentityBinding
    vector_mapping: str
    schedule_stability: ScheduleStabilityContract
    occurrences: tuple[EligibleOccurrence, ...]

    def validate(self) -> None:
        if self.schema_version != ELIGIBLE_WORKLOAD_SCHEMA_VERSION:
            raise ContractViolation("eligible workload schema version is invalid")
        _utc_timestamp(self.created_at_utc, field="eligible workload created_at_utc")
        _sha256(self.dataset002_manifest_sha256, field="dataset002_manifest_sha256")
        _sha256(
            self.dataset001_generation_manifest_sha256,
            field="dataset001_generation_manifest_sha256",
        )
        if not isinstance(self.metric, Metric):
            raise ContractViolation("eligible workload metric is invalid")
        if self.threshold_stratum not in THRESHOLD_LABELS:
            raise ContractViolation("eligible workload threshold stratum is invalid")
        _validate_transition(
            candidate_ef=self.candidate_ef,
            last_known_good_ef=self.last_known_good_ef,
        )
        radius = _finite_float(self.radius, field="eligible workload radius")
        range_filter = _finite_float(self.range_filter, field="eligible workload range_filter")
        if self.metric is Metric.L2:
            if not radius > 0.0 or range_filter != 0.0:
                raise ContractViolation("L2 workload range contract is invalid")
        elif not -1.0 <= radius < 1.0 or range_filter != 1.0:
            raise ContractViolation("COSINE workload range contract is invalid")
        if _integer(self.limit, field="eligible workload limit", minimum=1) != RESULT_LIMIT:
            raise ContractViolation("eligible workload limit is invalid")
        self.identity.validate()
        if self.vector_mapping != VECTOR_MAPPING_ONE_TO_ONE:
            raise ContractViolation("eligible workload vector mapping declaration is invalid")
        if not isinstance(self.schedule_stability, ScheduleStabilityContract):
            raise ContractViolation("eligible workload schedule stability contract is invalid")
        self.schedule_stability.validate()
        if self.schedule_stability.control_ef != self.last_known_good_ef:
            raise ContractViolation("schedule stability control_ef must equal last-known-good ef")
        if len(self.occurrences) != EXP009_ROUTING_POPULATION_COUNT:
            raise ContractViolation("eligible workload must contain exactly 600 occurrences")
        ids: set[str] = set()
        vector_hashes: set[str] = set()
        for expected_index, occurrence in enumerate(self.occurrences):
            occurrence.validate()
            if occurrence.sequence_index != expected_index:
                raise ContractViolation("eligible workload occurrence ordering is invalid")
            if occurrence.occurrence_id in ids or occurrence.vector_sha256 in vector_hashes:
                raise ContractViolation("eligible workload occurrence/vector uniqueness is invalid")
            if (
                occurrence.threshold_radius != radius
                or occurrence.range_filter != range_filter
                or occurrence.limit != self.limit
            ):
                raise ContractViolation("eligible workload occurrence search binding is invalid")
            ids.add(occurrence.occurrence_id)
            vector_hashes.add(occurrence.vector_sha256)
        if set(self.schedule_stability.control_vector_sha256) & vector_hashes:
            raise ContractViolation("schedule stability controls must be disjoint from routing vectors")

    def to_document(self) -> dict[str, object]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "created_at_utc": self.created_at_utc,
            "dataset": {
                "dataset002_manifest_sha256": self.dataset002_manifest_sha256,
                "dataset001_generation_manifest_sha256": self.dataset001_generation_manifest_sha256,
            },
            "search": {
                "metric": self.metric.value,
                "threshold_stratum": self.threshold_stratum,
                "candidate_ef": self.candidate_ef,
                "last_known_good_ef": self.last_known_good_ef,
                "radius": self.radius,
                "range_filter": self.range_filter,
                "limit": self.limit,
            },
            "identity": self.identity.to_document(),
            "vector_mapping": self.vector_mapping,
            "schedule_stability": self.schedule_stability.to_document(),
            "occurrences": [entry.to_document() for entry in self.occurrences],
        }


@dataclass(frozen=True, slots=True)
class CandidateSelectionRecord:
    """A canonical 60-of-600 CSPRNG selection bound to one frozen manifest."""

    schema_version: str
    selected_at_utc: str
    eligible_manifest_sha256: str
    population_count: int
    candidate_count: int
    candidate_fraction: float
    candidate_occurrence_ids: tuple[str, ...]
    random_source: str
    selected_before_candidate_results: bool

    def validate(self) -> None:
        if self.schema_version != CANDIDATE_SELECTION_SCHEMA_VERSION:
            raise ContractViolation("candidate selection record schema version is invalid")
        _utc_timestamp(self.selected_at_utc, field="candidate selection selected_at_utc")
        _sha256(self.eligible_manifest_sha256, field="eligible_manifest_sha256")
        if self.population_count != EXP009_ROUTING_POPULATION_COUNT:
            raise ContractViolation("candidate selection record population count is invalid")
        if self.candidate_count != EXP009_CANDIDATE_COUNT:
            raise ContractViolation("candidate selection record candidate count is invalid")
        if self.candidate_fraction != 0.10:
            raise ContractViolation("candidate selection record fraction must equal 0.10")
        if self.random_source != SYSTEM_RANDOM_SOURCE:
            raise ContractViolation("candidate selection record random source provenance is invalid")
        if self.selected_before_candidate_results is not True:
            raise ContractViolation("candidate selection record must precede candidate results")
        if len(self.candidate_occurrence_ids) != EXP009_CANDIDATE_COUNT:
            raise ContractViolation("candidate selection record must contain exactly 60 IDs")
        if len(set(self.candidate_occurrence_ids)) != EXP009_CANDIDATE_COUNT:
            raise ContractViolation("candidate selection record IDs must be unique")
        for occurrence_id in self.candidate_occurrence_ids:
            if not isinstance(occurrence_id, str) or _OCCURRENCE_RE.fullmatch(occurrence_id) is None:
                raise ContractViolation("candidate selection record contains a noncanonical ID")

    def to_document(self) -> dict[str, object]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "selected_at_utc": self.selected_at_utc,
            "eligible_manifest_sha256": self.eligible_manifest_sha256,
            "population_count": self.population_count,
            "candidate_count": self.candidate_count,
            "candidate_fraction": self.candidate_fraction,
            "candidate_occurrence_ids": list(self.candidate_occurrence_ids),
            "random_source": self.random_source,
            "selected_before_candidate_results": self.selected_before_candidate_results,
        }


def _read_json(path: Path, *, noun: str) -> Mapping[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_no_duplicate_json_fields,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except FileNotFoundError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, _DuplicateJsonField, ValueError) as exc:
        raise ContractViolation(f"{noun} is unreadable or malformed") from exc
    if not isinstance(value, Mapping):
        raise ContractViolation(f"{noun} root must be an object")
    if raw != canonical_json_bytes(value):
        raise ContractViolation(f"{noun} is not canonical JSON")
    return value


def _occurrence_from_document(value: object) -> EligibleOccurrence:
    payload = _exact_mapping(
        value,
        frozenset(
            {
                "sequence_index",
                "occurrence_id",
                "dataset_query_id",
                "vector_sha256",
                "threshold_radius",
                "range_filter",
                "limit",
            }
        ),
        field="eligible occurrence",
    )
    return EligibleOccurrence(
        sequence_index=_integer(payload["sequence_index"], field="occurrence.sequence_index", minimum=0),
        occurrence_id=_text(payload["occurrence_id"], field="occurrence.occurrence_id"),
        dataset_query_id=_integer(payload["dataset_query_id"], field="occurrence.dataset_query_id", minimum=0),
        vector_sha256=_sha256(payload["vector_sha256"], field="occurrence.vector_sha256"),
        threshold_radius=_finite_float(payload["threshold_radius"], field="occurrence.threshold_radius"),
        range_filter=_finite_float(payload["range_filter"], field="occurrence.range_filter"),
        limit=_integer(payload["limit"], field="occurrence.limit", minimum=1),
    )


def _schedule_stability_from_document(value: object) -> ScheduleStabilityContract:
    payload = _exact_mapping(
        value,
        frozenset(
            {
                "schema_version",
                "control_role",
                "control_ef",
                "controls",
                "pre_sweep_count",
                "routing_block_size",
                "interleaved_sweep_count",
                "post_sweep_count",
                "execution_mode",
                "absolute_p95_latency_ms_ceiling",
                "p95_relative_ceiling",
                "median_relative_ceiling",
                "require_all_success",
                "require_identity_and_health_per_sweep",
            }
        ),
        field="schedule stability contract",
    )
    controls = payload["controls"]
    if not isinstance(controls, list):
        raise ContractViolation("schedule stability controls must be an array")
    parsed_controls: list[ScheduleControl] = []
    for item in controls:
        control = _exact_mapping(
            item,
            frozenset({"query_id", "vector_sha256"}),
            field="schedule stability control",
        )
        parsed_controls.append(
            ScheduleControl(
                query_id=_integer(control["query_id"], field="schedule control query_id", minimum=0),
                vector_sha256=_sha256(
                    control["vector_sha256"], field="schedule control vector_sha256"
                ),
            )
        )
    contract = ScheduleStabilityContract(
        schema_version=_text(payload["schema_version"], field="schedule stability schema_version"),
        control_role=_text(payload["control_role"], field="schedule stability control_role"),
        control_ef=_integer(payload["control_ef"], field="schedule stability control_ef", minimum=1),
        controls=tuple(parsed_controls),
        pre_sweep_count=_integer(
            payload["pre_sweep_count"], field="schedule stability pre_sweep_count", minimum=1
        ),
        routing_block_size=_integer(
            payload["routing_block_size"],
            field="schedule stability routing_block_size",
            minimum=1,
        ),
        interleaved_sweep_count=_integer(
            payload["interleaved_sweep_count"],
            field="schedule stability interleaved_sweep_count",
            minimum=1,
        ),
        post_sweep_count=_integer(
            payload["post_sweep_count"], field="schedule stability post_sweep_count", minimum=1
        ),
        execution_mode=_text(payload["execution_mode"], field="schedule stability execution_mode"),
        absolute_p95_latency_ms_ceiling=_finite_float(
            payload["absolute_p95_latency_ms_ceiling"],
            field="schedule stability absolute_p95_latency_ms_ceiling",
        ),
        p95_relative_ceiling=_finite_float(
            payload["p95_relative_ceiling"], field="schedule stability p95_relative_ceiling"
        ),
        median_relative_ceiling=_finite_float(
            payload["median_relative_ceiling"], field="schedule stability median_relative_ceiling"
        ),
        require_all_success=payload["require_all_success"],
        require_identity_and_health_per_sweep=payload[
            "require_identity_and_health_per_sweep"
        ],
    )
    contract.validate()
    return contract


def _manifest_from_document(document: Mapping[str, object]) -> EligibleWorkloadManifest:
    root = _exact_mapping(
        document,
        frozenset(
            {
                "schema_version",
                "created_at_utc",
                "dataset",
                "search",
                "identity",
                "vector_mapping",
                "schedule_stability",
                "occurrences",
            }
        ),
        field="eligible workload manifest",
    )
    dataset = _exact_mapping(
        root["dataset"],
        frozenset({"dataset002_manifest_sha256", "dataset001_generation_manifest_sha256"}),
        field="eligible workload dataset",
    )
    search = _exact_mapping(
        root["search"],
        frozenset(
            {
                "metric",
                "threshold_stratum",
                "candidate_ef",
                "last_known_good_ef",
                "radius",
                "range_filter",
                "limit",
            }
        ),
        field="eligible workload search",
    )
    identity = _exact_mapping(
        root["identity"],
        frozenset(
            {
                "configuration_identity",
                "data_identity",
                "flat_binding_id",
                "hnsw_binding_id",
            }
        ),
        field="eligible workload identity",
    )
    occurrences = root["occurrences"]
    if not isinstance(occurrences, list):
        raise ContractViolation("eligible workload occurrences must be an array")
    manifest = EligibleWorkloadManifest(
        schema_version=_text(root["schema_version"], field="eligible workload schema_version"),
        created_at_utc=_utc_timestamp(root["created_at_utc"], field="eligible workload created_at_utc"),
        dataset002_manifest_sha256=_sha256(dataset["dataset002_manifest_sha256"], field="dataset002_manifest_sha256"),
        dataset001_generation_manifest_sha256=_sha256(dataset["dataset001_generation_manifest_sha256"], field="dataset001_generation_manifest_sha256"),
        metric=_metric(search["metric"], field="eligible workload metric"),
        threshold_stratum=_text(search["threshold_stratum"], field="eligible workload threshold_stratum"),
        candidate_ef=_integer(search["candidate_ef"], field="candidate_ef", minimum=1),
        last_known_good_ef=_integer(search["last_known_good_ef"], field="last_known_good_ef", minimum=1),
        radius=_finite_float(search["radius"], field="eligible workload radius"),
        range_filter=_finite_float(search["range_filter"], field="eligible workload range_filter"),
        limit=_integer(search["limit"], field="eligible workload limit", minimum=1),
        identity=WorkloadIdentityBinding(
            configuration_identity=_text(identity["configuration_identity"], field="configuration_identity"),
            data_identity=_text(identity["data_identity"], field="data_identity"),
            flat_binding_id=_text(identity["flat_binding_id"], field="flat_binding_id"),
            hnsw_binding_id=_text(identity["hnsw_binding_id"], field="hnsw_binding_id"),
        ),
        vector_mapping=_text(root["vector_mapping"], field="eligible workload vector_mapping"),
        schedule_stability=_schedule_stability_from_document(root["schedule_stability"]),
        occurrences=tuple(_occurrence_from_document(item) for item in occurrences),
    )
    manifest.validate()
    return manifest


def _selection_from_document(document: Mapping[str, object]) -> CandidateSelectionRecord:
    root = _exact_mapping(
        document,
        frozenset(
            {
                "schema_version",
                "selected_at_utc",
                "eligible_manifest_sha256",
                "population_count",
                "candidate_count",
                "candidate_fraction",
                "candidate_occurrence_ids",
                "random_source",
                "selected_before_candidate_results",
            }
        ),
        field="candidate selection record",
    )
    ids = root["candidate_occurrence_ids"]
    if not isinstance(ids, list):
        raise ContractViolation("candidate selection record IDs must be an array")
    record = CandidateSelectionRecord(
        schema_version=_text(root["schema_version"], field="candidate selection schema_version"),
        selected_at_utc=_utc_timestamp(root["selected_at_utc"], field="candidate selection selected_at_utc"),
        eligible_manifest_sha256=_sha256(root["eligible_manifest_sha256"], field="eligible_manifest_sha256"),
        population_count=_integer(root["population_count"], field="population_count", minimum=1),
        candidate_count=_integer(root["candidate_count"], field="candidate_count", minimum=1),
        candidate_fraction=_finite_float(root["candidate_fraction"], field="candidate_fraction"),
        candidate_occurrence_ids=tuple(ids),
        random_source=_text(root["random_source"], field="random_source"),
        selected_before_candidate_results=root["selected_before_candidate_results"],
    )
    try:
        record.validate()
    except ContractViolation as exc:
        raise ContractViolation(f"candidate selection record is invalid: {exc}") from exc
    return record


def _routing_oracle_contract(
    *,
    dataset002_dir: Path,
    metric: Metric,
    threshold_stratum: str,
) -> tuple[float, float, int]:
    values: set[tuple[float, float, int]] = set()
    try:
        lines = (dataset002_dir / "oracle_records.jsonl").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ContractViolation("DATASET-002 oracle records are unreadable") from exc
    for line in lines:
        try:
            record = json.loads(line, object_pairs_hook=_no_duplicate_json_fields)
        except (json.JSONDecodeError, _DuplicateJsonField) as exc:
            raise ContractViolation("DATASET-002 oracle record is malformed") from exc
        if not isinstance(record, Mapping):
            raise ContractViolation("DATASET-002 oracle record is malformed")
        if (
            record.get("role") == "routing"
            and record.get("metric") == metric.value
            and record.get("threshold_label") == threshold_stratum
        ):
            values.add(
                (
                    _finite_float(record.get("radius"), field="oracle radius"),
                    _finite_float(record.get("range_filter"), field="oracle range_filter"),
                    _integer(record.get("limit"), field="oracle limit", minimum=1),
                )
            )
    if len(values) != 1:
        raise ContractViolation("DATASET-002 routing oracle contract is incomplete or inconsistent")
    return next(iter(values))


def _schedule_stability_contract(
    *, dataset002_dir: Path, last_known_good_ef: int
) -> ScheduleStabilityContract:
    """Bind the frozen Stage-4 LKG controls without exposing query vectors."""

    try:
        ids = np.load(dataset002_dir / "recall_audit_ids.npy", allow_pickle=False)
        vectors = np.load(dataset002_dir / "recall_audit_queries.npy", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise ContractViolation("DATASET-002 schedule-control arrays are unreadable") from exc
    expected_ids = np.arange(
        EXP009_ROUTING_POPULATION_COUNT,
        EXP009_ROUTING_POPULATION_COUNT + SCHEDULE_CONTROL_COUNT,
        dtype=np.int64,
    )
    if (
        ids.ndim != 1
        or vectors.ndim != 2
        or ids.shape[0] < SCHEDULE_CONTROL_COUNT
        or vectors.shape[0] < SCHEDULE_CONTROL_COUNT
        or ids.dtype.str != "<i8"
        or vectors.dtype.str != "<f4"
        or not np.array_equal(ids[:SCHEDULE_CONTROL_COUNT], expected_ids)
        or not np.all(np.isfinite(vectors[:SCHEDULE_CONTROL_COUNT]))
    ):
        raise ContractViolation("DATASET-002 schedule-control arrays violate the frozen contract")
    controls = tuple(
        ScheduleControl(
            query_id=int(query_id),
            vector_sha256=hashlib.sha256(
                np.ascontiguousarray(vector, dtype="<f4").tobytes(order="C")
            ).hexdigest(),
        )
        for query_id, vector in zip(
            ids[:SCHEDULE_CONTROL_COUNT], vectors[:SCHEDULE_CONTROL_COUNT], strict=True
        )
    )
    result = ScheduleStabilityContract(
        schema_version=SCHEDULE_STABILITY_SCHEMA_VERSION,
        control_role=SCHEDULE_CONTROL_ROLE,
        control_ef=last_known_good_ef,
        controls=controls,
        pre_sweep_count=SCHEDULE_PRE_SWEEP_COUNT,
        routing_block_size=SCHEDULE_ROUTING_BLOCK_SIZE,
        interleaved_sweep_count=SCHEDULE_INTERLEAVED_SWEEP_COUNT,
        post_sweep_count=SCHEDULE_POST_SWEEP_COUNT,
        execution_mode=SCHEDULE_EXECUTION_MODE,
        absolute_p95_latency_ms_ceiling=SCHEDULE_ABSOLUTE_P95_LATENCY_MS_CEILING,
        p95_relative_ceiling=SCHEDULE_P95_RELATIVE_CEILING,
        median_relative_ceiling=SCHEDULE_MEDIAN_RELATIVE_CEILING,
        require_all_success=True,
        require_identity_and_health_per_sweep=True,
    )
    result.validate()
    return result


def build_eligible_workload_manifest(
    *,
    dataset002_dir: str | os.PathLike[str],
    dataset001_dir: str | os.PathLike[str],
    metric: Metric | str,
    threshold_stratum: str,
    candidate_ef: int,
    last_known_good_ef: int,
    identity: WorkloadIdentityBinding,
    created_at_utc: str,
) -> EligibleWorkloadManifest:
    """Build one in-memory 600-occurrence manifest from verified datasets only."""

    dataset002_path = Path(dataset002_dir)
    dataset001_path = Path(dataset001_dir)
    manifest = verify_dataset002_artifacts(dataset002_path, dataset001_dir=dataset001_path)
    normalized_metric = _metric(metric, field="metric")
    if threshold_stratum not in THRESHOLD_LABELS:
        raise ContractViolation("threshold_stratum is not frozen by EXP-001")
    created = _utc_timestamp(created_at_utc, field="eligible workload created_at_utc")
    candidate, lkg = _validate_transition(
        candidate_ef=candidate_ef,
        last_known_good_ef=last_known_good_ef,
    )
    if not isinstance(identity, WorkloadIdentityBinding):
        raise TypeError("identity must be a WorkloadIdentityBinding")
    identity.validate()
    inherited = manifest.get("inherited_dataset001")
    if not isinstance(inherited, Mapping):
        raise ContractViolation("DATASET-002 inherited DATASET-001 identity is invalid")
    expected_data_identity = _expected_data_identity(inherited)
    if identity.data_identity != expected_data_identity:
        raise ContractViolation("data_identity must bind the inherited DATASET-001 manifest")
    try:
        ids = np.load(dataset002_path / "routing_ids.npy", allow_pickle=False)
        vectors = np.load(dataset002_path / "routing_queries.npy", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise ContractViolation("DATASET-002 routing arrays are unreadable") from exc
    if (
        ids.shape != (EXP009_ROUTING_POPULATION_COUNT,)
        or vectors.ndim != 2
        or vectors.shape[0] != EXP009_ROUTING_POPULATION_COUNT
        or ids.dtype.str != "<i8"
        or vectors.dtype.str != "<f4"
        or not np.all(np.isfinite(vectors))
    ):
        raise ContractViolation("DATASET-002 routing arrays violate the 600-occurrence contract")
    radius, range_filter, limit = _routing_oracle_contract(
        dataset002_dir=dataset002_path,
        metric=normalized_metric,
        threshold_stratum=threshold_stratum,
    )
    if limit != RESULT_LIMIT:
        raise ContractViolation("DATASET-002 routing oracle limit is incompatible")
    schedule_stability = _schedule_stability_contract(
        dataset002_dir=dataset002_path,
        last_known_good_ef=lkg,
    )
    occurrences: list[EligibleOccurrence] = []
    for sequence_index, (query_id, vector) in enumerate(zip(ids, vectors, strict=True)):
        integer_id = int(query_id)
        if integer_id != sequence_index:
            raise ContractViolation("DATASET-002 routing IDs must be canonical 0..599")
        vector_digest = hashlib.sha256(
            np.ascontiguousarray(vector, dtype="<f4").tobytes(order="C")
        ).hexdigest()
        occurrences.append(
            EligibleOccurrence(
                sequence_index=sequence_index,
                occurrence_id=f"exp009-routing-{integer_id:06d}",
                dataset_query_id=integer_id,
                vector_sha256=vector_digest,
                threshold_radius=radius,
                range_filter=range_filter,
                limit=limit,
            )
        )
    result = EligibleWorkloadManifest(
        schema_version=ELIGIBLE_WORKLOAD_SCHEMA_VERSION,
        created_at_utc=created,
        dataset002_manifest_sha256=sha256_file(dataset002_path / "dataset002_manifest.json"),
        dataset001_generation_manifest_sha256=_sha256(
            inherited.get("generation_manifest_sha256"),
            field="inherited_dataset001.generation_manifest_sha256",
        ),
        metric=normalized_metric,
        threshold_stratum=threshold_stratum,
        candidate_ef=candidate,
        last_known_good_ef=lkg,
        radius=radius,
        range_filter=range_filter,
        limit=limit,
        identity=identity,
        vector_mapping=VECTOR_MAPPING_ONE_TO_ONE,
        schedule_stability=schedule_stability,
        occurrences=tuple(occurrences),
    )
    result.validate()
    return result


def _persist_immutable_json(path: Path, document: Mapping[str, object]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite immutable evidence artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(canonical_json_bytes(document))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            raise FileExistsError(f"refusing to overwrite immutable evidence artifact: {path}") from None
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def persist_eligible_workload_manifest(
    path: str | os.PathLike[str], manifest: EligibleWorkloadManifest
) -> None:
    """Persist an immutable canonical manifest before any candidate selection."""

    if not isinstance(manifest, EligibleWorkloadManifest):
        raise TypeError("manifest must be an EligibleWorkloadManifest")
    document = manifest.to_document()
    if _manifest_from_document(document) != manifest:
        raise ContractViolation("eligible workload manifest fails self-validation")
    _persist_immutable_json(Path(path), document)


def load_eligible_workload_manifest(
    path: str | os.PathLike[str],
) -> EligibleWorkloadManifest:
    """Load one canonical workload artifact without trusting its filename."""

    return _manifest_from_document(_read_json(Path(path), noun="eligible workload manifest"))


def verify_eligible_workload_manifest(
    path: str | os.PathLike[str],
    *,
    dataset002_dir: str | os.PathLike[str],
    dataset001_dir: str | os.PathLike[str],
) -> EligibleWorkloadManifest:
    """Rebuild a manifest from its datasets and reject any hidden substitution."""

    loaded = load_eligible_workload_manifest(path)
    rebuilt = build_eligible_workload_manifest(
        dataset002_dir=dataset002_dir,
        dataset001_dir=dataset001_dir,
        metric=loaded.metric,
        threshold_stratum=loaded.threshold_stratum,
        candidate_ef=loaded.candidate_ef,
        last_known_good_ef=loaded.last_known_good_ef,
        identity=loaded.identity,
        created_at_utc=loaded.created_at_utc,
    )
    if rebuilt.to_document() != loaded.to_document():
        raise ContractViolation("eligible workload manifest disagrees with verified DATASET artifacts")
    return loaded


def create_candidate_selection_record(
    eligible_manifest_path: str | os.PathLike[str],
    *,
    selected_at_utc: str,
) -> CandidateSelectionRecord:
    """CSPRNG-select exactly 60 IDs from an already persisted 600-ID manifest.

    The function accepts a path rather than an in-memory manifest so selection
    cannot precede the immutable-workload publication boundary by accident.
    Candidate outcomes are neither accepted nor read by this offline function.
    """

    path = Path(eligible_manifest_path)
    workload = load_eligible_workload_manifest(path)
    selected_at = _utc_timestamp(selected_at_utc, field="candidate selection selected_at_utc")
    if _timestamp_instant(selected_at) <= _timestamp_instant(workload.created_at_utc):
        raise ContractViolation("candidate selection must occur strictly after workload freeze")
    population = [occurrence.occurrence_id for occurrence in workload.occurrences]
    drawn = secrets.SystemRandom().sample(population, EXP009_CANDIDATE_COUNT)
    if len(drawn) != EXP009_CANDIDATE_COUNT or len(set(drawn)) != EXP009_CANDIDATE_COUNT:
        raise ContractViolation("CSPRNG did not return a unique 60-ID selection")
    candidate_set = set(drawn)
    canonical_ids = tuple(occurrence_id for occurrence_id in population if occurrence_id in candidate_set)
    record = CandidateSelectionRecord(
        schema_version=CANDIDATE_SELECTION_SCHEMA_VERSION,
        selected_at_utc=selected_at,
        eligible_manifest_sha256=sha256_file(path),
        population_count=EXP009_ROUTING_POPULATION_COUNT,
        candidate_count=EXP009_CANDIDATE_COUNT,
        candidate_fraction=0.10,
        candidate_occurrence_ids=canonical_ids,
        random_source=SYSTEM_RANDOM_SOURCE,
        selected_before_candidate_results=True,
    )
    record.validate()
    return record


def persist_candidate_selection_record(
    path: str | os.PathLike[str],
    record: CandidateSelectionRecord,
    eligible_manifest_path: str | os.PathLike[str],
) -> None:
    """Persist one immutable selection record only after full binding validation."""

    if not isinstance(record, CandidateSelectionRecord):
        raise TypeError("record must be a CandidateSelectionRecord")
    verify_candidate_selection_record_value(record, eligible_manifest_path)
    document = record.to_document()
    if _selection_from_document(document) != record:
        raise ContractViolation("candidate selection record fails self-validation")
    _persist_immutable_json(Path(path), document)


def load_candidate_selection_record(
    path: str | os.PathLike[str],
) -> CandidateSelectionRecord:
    """Load a strict selection record; manifest binding is verified separately."""

    return _selection_from_document(_read_json(Path(path), noun="candidate selection record"))


def verify_candidate_selection_record_value(
    record: CandidateSelectionRecord,
    eligible_manifest_path: str | os.PathLike[str],
) -> CandidateSelectionRecord:
    """Validate a selection record against the exact frozen manifest file."""

    if not isinstance(record, CandidateSelectionRecord):
        raise TypeError("record must be a CandidateSelectionRecord")
    record.validate()
    path = Path(eligible_manifest_path)
    workload = load_eligible_workload_manifest(path)
    if record.eligible_manifest_sha256 != sha256_file(path):
        raise ContractViolation("candidate selection record manifest digest mismatch")
    if _timestamp_instant(record.selected_at_utc) <= _timestamp_instant(workload.created_at_utc):
        raise ContractViolation("candidate selection record predates workload freeze")
    eligible_ids = tuple(occurrence.occurrence_id for occurrence in workload.occurrences)
    eligible_set = set(eligible_ids)
    if not set(record.candidate_occurrence_ids).issubset(eligible_set):
        raise ContractViolation("candidate selection record contains IDs outside the eligible manifest")
    expected_order = tuple(
        occurrence_id
        for occurrence_id in eligible_ids
        if occurrence_id in set(record.candidate_occurrence_ids)
    )
    if record.candidate_occurrence_ids != expected_order:
        raise ContractViolation("candidate selection record IDs are not in canonical manifest order")
    return record


def verify_candidate_selection_record(
    path: str | os.PathLike[str],
    eligible_manifest_path: str | os.PathLike[str],
) -> CandidateSelectionRecord:
    """Load and bind-verify one persisted candidate-selection evidence artifact."""

    return verify_candidate_selection_record_value(
        load_candidate_selection_record(path),
        eligible_manifest_path,
    )


__all__ = [
    "CANDIDATE_SELECTION_SCHEMA_VERSION",
    "ELIGIBLE_WORKLOAD_SCHEMA_VERSION",
    "SCHEDULE_STABILITY_SCHEMA_VERSION",
    "CandidateSelectionRecord",
    "EligibleOccurrence",
    "EligibleWorkloadManifest",
    "ScheduleControl",
    "ScheduleStabilityContract",
    "WorkloadIdentityBinding",
    "build_eligible_workload_manifest",
    "create_candidate_selection_record",
    "load_candidate_selection_record",
    "load_eligible_workload_manifest",
    "persist_candidate_selection_record",
    "persist_eligible_workload_manifest",
    "verify_candidate_selection_record",
    "verify_candidate_selection_record_value",
    "verify_eligible_workload_manifest",
]
