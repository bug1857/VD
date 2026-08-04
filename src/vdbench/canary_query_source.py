"""Verified DATASET-002 vector source for the EXP-009 Stage-4 runner.

Purpose:
    Revalidate the persisted DATASET-002 artifact once, bind every routing and
    schedule-control vector to the admitted immutable workload, and expose
    only per-record immutable vector lookups to a later serial runner.
Inputs:
    DATASET-001/DATASET-002 artifact directories and one validated
    ``EligibleWorkloadManifest``.
Outputs:
    Tuple-form float32-equivalent vectors for exact manifest occurrences and
    schedule controls or the separate 1,200-query recall-audit population; no
    bulk raw-array export is provided.
Dependencies:
    The existing DATASET-002 verifier and immutable workload values.  This
    source deliberately has no route-authority, policy, activation, Milvus,
    or query-execution dependency.
Failure modes:
    Any checksum/schema/oracle failure, non-finite value, missing ID, or
    occurrence/control binding mismatch raises a stable fail-closed error.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import math
from pathlib import Path
from types import MappingProxyType

import numpy as np

from .artifacts import sha256_file
from .canary_statistics import (
    EXP009_RECALL_AUDIT_COUNT,
    EXP009_ROUTING_POPULATION_COUNT,
)
from .canary_workload import EligibleOccurrence, EligibleWorkloadManifest, ScheduleControl
from .config import ContractViolation
from .dataset002 import verify_dataset002_artifacts


__all__ = [
    "CanaryQuerySourceError",
    "Dataset002CanaryQuerySource",
]


class CanaryQuerySourceError(RuntimeError):
    """A non-sensitive refusal from the Stage-4 verified vector boundary."""


class Dataset002CanaryQuerySource:
    """Read-only exact-vector lookup built only from verified DATASET-002 bytes."""

    def __init__(
        self,
        *,
        routing: Mapping[str, tuple[EligibleOccurrence, tuple[float, ...]]],
        recall_audits: Mapping[int, tuple[str, tuple[float, ...]]],
        controls: Mapping[int, tuple[str, tuple[float, ...]]],
    ) -> None:
        self._routing = MappingProxyType(dict(routing))
        self._recall_audits = MappingProxyType(dict(recall_audits))
        self._controls = MappingProxyType(dict(controls))

    @classmethod
    def from_verified_artifacts(
        cls,
        *,
        dataset002_dir: str | Path,
        dataset001_dir: str | Path,
        manifest: EligibleWorkloadManifest,
    ) -> "Dataset002CanaryQuerySource":
        """Verify artifacts, then bind each in-memory vector to the manifest.

        Verification intentionally happens during construction, off the future
        foreground loop. The returned object has no path references and never
        rereads artifacts while a request is being served.
        """

        if not isinstance(manifest, EligibleWorkloadManifest):
            raise CanaryQuerySourceError("WORKLOAD_MANIFEST_INVALID")
        try:
            manifest.validate()
            dataset002_path = Path(dataset002_dir)
            verify_dataset002_artifacts(dataset002_path, dataset001_dir=dataset001_dir)
        except (ContractViolation, OSError, TypeError, ValueError):
            raise CanaryQuerySourceError("DATASET002_VERIFICATION_FAILED") from None

        try:
            manifest_sha256 = sha256_file(dataset002_path / "dataset002_manifest.json")
        except OSError:
            raise CanaryQuerySourceError("DATASET002_MANIFEST_BINDING_UNAVAILABLE") from None
        if manifest_sha256 != manifest.dataset002_manifest_sha256:
            raise CanaryQuerySourceError("DATASET002_MANIFEST_BINDING_MISMATCH")
        try:
            routing_ids = np.load(dataset002_path / "routing_ids.npy", allow_pickle=False)
            routing_queries = np.load(
                dataset002_path / "routing_queries.npy", allow_pickle=False
            )
            control_ids = np.load(
                dataset002_path / "recall_audit_ids.npy", allow_pickle=False
            )
            control_queries = np.load(
                dataset002_path / "recall_audit_queries.npy", allow_pickle=False
            )
        except (OSError, ValueError):
            raise CanaryQuerySourceError("DATASET002_ARRAY_LOAD_FAILED") from None

        routing = cls._bind_routing(manifest, routing_ids, routing_queries)
        recall_audits = cls._bind_recall_audits(control_ids, control_queries)
        controls = cls._bind_controls(manifest, recall_audits)
        return cls(
            routing=routing,
            recall_audits=recall_audits,
            controls=controls,
        )

    @property
    def routing_count(self) -> int:
        """Return the frozen routing population size, without exposing it."""

        return len(self._routing)

    @property
    def control_count(self) -> int:
        """Return the frozen schedule-control population size."""

        return len(self._controls)

    @property
    def recall_audit_count(self) -> int:
        """Return the disjoint DATASET-002 recall-audit population size."""

        return len(self._recall_audits)

    def routing_vector(
        self,
        *,
        occurrence_id: str,
        dataset_query_id: int,
        vector_sha256: str,
    ) -> tuple[float, ...]:
        """Return one routing vector only for its complete manifest binding.

        The scalar contract deliberately matches the route plan's public
        occurrence fields without importing the routing module or accepting an
        authority/claim object. This prevents the source from becoming a
        second routing authority while keeping the future runner type-safe.
        """

        if (
            not isinstance(occurrence_id, str)
            or isinstance(dataset_query_id, bool)
            or not isinstance(dataset_query_id, int)
            or not isinstance(vector_sha256, str)
        ):
            raise CanaryQuerySourceError("ROUTING_OCCURRENCE_INVALID")
        stored = self._routing.get(occurrence_id)
        if (
            stored is None
            or stored[0].dataset_query_id != dataset_query_id
            or stored[0].vector_sha256 != vector_sha256
        ):
            raise CanaryQuerySourceError("ROUTING_OCCURRENCE_MISMATCH")
        return stored[1]

    def control_vector(self, *, control: ScheduleControl) -> tuple[float, ...]:
        """Return exactly one pre-registered LKG schedule-control vector."""

        if not isinstance(control, ScheduleControl):
            raise CanaryQuerySourceError("SCHEDULE_CONTROL_INVALID")
        stored = self._controls.get(control.query_id)
        if stored is None or stored[0] != control.vector_sha256:
            raise CanaryQuerySourceError("CONTROL_BINDING_MISMATCH")
        return stored[1]

    def recall_audit_vector(self, *, query_id: int) -> tuple[float, ...]:
        """Return one exact DATASET-002 recall-audit vector by canonical ID."""

        if isinstance(query_id, bool) or not isinstance(query_id, int):
            raise CanaryQuerySourceError("RECALL_AUDIT_QUERY_ID_INVALID")
        stored = self._recall_audits.get(query_id)
        if stored is None:
            raise CanaryQuerySourceError("RECALL_AUDIT_QUERY_ID_UNKNOWN")
        return stored[1]

    @staticmethod
    def _bind_routing(
        manifest: EligibleWorkloadManifest,
        identifiers: object,
        vectors: object,
    ) -> dict[str, tuple[EligibleOccurrence, tuple[float, ...]]]:
        if not isinstance(identifiers, np.ndarray) or not isinstance(vectors, np.ndarray):
            raise CanaryQuerySourceError("ROUTING_ARRAY_SCHEMA_INVALID")
        if (
            identifiers.shape != (EXP009_ROUTING_POPULATION_COUNT,)
            or identifiers.dtype.str != "<i8"
            or vectors.ndim != 2
            or vectors.shape[0] != EXP009_ROUTING_POPULATION_COUNT
            or vectors.dtype.str != "<f4"
            or not np.all(np.isfinite(vectors))
        ):
            raise CanaryQuerySourceError("ROUTING_ARRAY_SCHEMA_INVALID")
        result: dict[str, tuple[EligibleOccurrence, tuple[float, ...]]] = {}
        for expected, query_id, vector in zip(
            manifest.occurrences, identifiers, vectors, strict=True
        ):
            if isinstance(query_id, np.bool_) or int(query_id) != expected.dataset_query_id:
                raise CanaryQuerySourceError("ROUTING_QUERY_ID_MISMATCH")
            normalized = _vector(vector)
            if _vector_sha256(normalized) != expected.vector_sha256:
                raise CanaryQuerySourceError("ROUTING_VECTOR_BINDING_MISMATCH")
            result[expected.occurrence_id] = (expected, normalized)
        if len(result) != len(manifest.occurrences):
            raise CanaryQuerySourceError("ROUTING_OCCURRENCE_DUPLICATE")
        return result

    @staticmethod
    def _bind_recall_audits(
        identifiers: object,
        vectors: object,
    ) -> dict[int, tuple[str, tuple[float, ...]]]:
        if not isinstance(identifiers, np.ndarray) or not isinstance(vectors, np.ndarray):
            raise CanaryQuerySourceError("RECALL_AUDIT_ARRAY_SCHEMA_INVALID")
        if (
            identifiers.shape != (EXP009_RECALL_AUDIT_COUNT,)
            or identifiers.dtype.str != "<i8"
            or vectors.ndim != 2
            or vectors.shape[0] != EXP009_RECALL_AUDIT_COUNT
            or vectors.dtype.str != "<f4"
            or not np.all(np.isfinite(vectors))
        ):
            raise CanaryQuerySourceError("RECALL_AUDIT_ARRAY_SCHEMA_INVALID")
        result: dict[int, tuple[str, tuple[float, ...]]] = {}
        for query_id, vector in zip(identifiers, vectors, strict=True):
            identifier = int(query_id)
            if (
                isinstance(query_id, np.bool_)
                or identifier not in range(
                    EXP009_ROUTING_POPULATION_COUNT,
                    EXP009_ROUTING_POPULATION_COUNT + EXP009_RECALL_AUDIT_COUNT,
                )
                or identifier in result
            ):
                raise CanaryQuerySourceError("RECALL_AUDIT_QUERY_ID_DUPLICATE")
            normalized = _vector(vector)
            result[identifier] = (_vector_sha256(normalized), normalized)
        if len(result) != EXP009_RECALL_AUDIT_COUNT:
            raise CanaryQuerySourceError("RECALL_AUDIT_QUERY_ID_DUPLICATE")
        return result

    @staticmethod
    def _bind_controls(
        manifest: EligibleWorkloadManifest,
        recall_audits: Mapping[int, tuple[str, tuple[float, ...]]],
    ) -> dict[int, tuple[str, tuple[float, ...]]]:
        result: dict[int, tuple[str, tuple[float, ...]]] = {}
        for expected in manifest.schedule_stability.controls:
            stored = recall_audits.get(expected.query_id)
            if stored is None:
                raise CanaryQuerySourceError("CONTROL_QUERY_ID_MISMATCH")
            if stored[0] != expected.vector_sha256:
                raise CanaryQuerySourceError("CONTROL_VECTOR_BINDING_MISMATCH")
            result[expected.query_id] = stored
        if len(result) != len(manifest.schedule_stability.controls):
            raise CanaryQuerySourceError("CONTROL_QUERY_ID_DUPLICATE")
        return result


def _vector(value: object) -> tuple[float, ...]:
    """Return canonical float32 bytes as an immutable tuple, never a view."""

    if not isinstance(value, np.ndarray) or value.ndim != 1:
        raise CanaryQuerySourceError("VECTOR_SCHEMA_INVALID")
    normalized = np.ascontiguousarray(value, dtype="<f4")
    if not normalized.size or not np.all(np.isfinite(normalized)):
        raise CanaryQuerySourceError("VECTOR_NONFINITE")
    result = tuple(float(item) for item in normalized)
    if not all(math.isfinite(item) for item in result):
        raise CanaryQuerySourceError("VECTOR_NONFINITE")
    return result


def _vector_sha256(vector: tuple[float, ...]) -> str:
    """Hash exact little-endian float32 vector bytes, matching Stage-1."""

    return hashlib.sha256(np.asarray(vector, dtype="<f4").tobytes(order="C")).hexdigest()
