"""ADR-014 production v2 host composition root (offline structure).

Purpose:
    Wire the accepted v2 path end to end without contacting any service:
    application request -> injected serving executor -> `ReferenceV2Host` ->
    durable source membership -> independent shadow cursor -> `V2ShadowWorker`
    -> `GovernedV2DetectorEvaluator` -> detector-v2 store -> real attestation
    store -> `VerifiedRealDetectorHead` -> independent EXP-010 cursor.
ADR-007:
    This is a *sibling* of `ReferenceRangeGateway`, never a modification of it.
    `HostObservationRecorder.offer()` is not called, wrapped, or altered here,
    and no synchronous v2 durability is inserted into that path.
Identity pinning (ADR-014 item 10):
    `data_identity` is derived mechanically from a `verify_dataset_artifacts`
    -verified DATASET-001 corpus as `<version>:sha256:<generation manifest
    digest>`. A caller-supplied literal is refused, which is what keeps a
    captured population consumable by `response_profile_oracle_producer`.
Live boundary:
    Serving and shadow capture are injected ports. This module constructs no
    PyMilvus client and performs no search; a real operator composition
    supplies real executors under separate authorization.
Authority:
    None. No policy, grant, routing, admission, activation, actuation, or
    candidate authority is created or imported.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .artifacts import sha256_file, verify_dataset_artifacts
from .config import ContractViolation, Metric
from .host_observation import RangeQueryRequest, RangeServingExecutor
from .host_window_detector_v2 import SQLiteHostWindowDetectorV2Store
from .host_window_lineage import (
    ReferenceV2Host,
    SQLiteHostResponseCommitStore,
    V2GenuineWorkloadObservationSource,
    V2VisibleResponse,
)
from .real_detector_attestation import GovernedV2DetectorEvaluator
from .real_detector_attestation_store import (
    SQLiteRealDetectorAttestationStore,
    VerifiedRealDetectorHead,
)
from .shadow_event_types import MonitorStreamKey
from .shadow_attempt_store import SQLiteShadowAttemptStore
from .v2_shadow_worker import V2ShadowCaptureExecutor, V2ShadowWorker
from .window_finalization import SQLiteWindowFinalizationStore


__all__ = [
    "Exp010V2HostError",
    "PinnedDatasetIdentity",
    "pin_dataset001_identity",
    "Exp010V2HostComposition",
    "SHADOW_CONSUMER_ID",
    "EXP010_CONSUMER_ID",
]


SHADOW_CONSUMER_ID = "v2-shadow"
EXP010_CONSUMER_ID = "v2-exp010"
_EXPECTED_DATASET_ID = "DATASET-001"


class Exp010V2HostError(RuntimeError):
    """Fail-closed composition error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010V2HostError:
    return Exp010V2HostError(code, message)


@dataclass(frozen=True, slots=True)
class PinnedDatasetIdentity:
    """A DATASET-001 identity proven from artifacts, never asserted."""

    data_identity: str
    generation_manifest_sha256: str
    base_vectors_sha256: str
    dimensions: int
    version: str


def pin_dataset001_identity(dataset001_dir: Path) -> PinnedDatasetIdentity:
    """Verify the corpus and derive its governed data identity mechanically.

    This is the single pinning point required by ADR-014 item 10: the derived
    string is what `response_profile_oracle_producer` will later re-derive and
    require, so a free-form `data_identity` can never enter the real path.
    """

    directory = Path(dataset001_dir)
    try:
        manifest = verify_dataset_artifacts(directory)
    except (OSError, ValueError, KeyError, ContractViolation) as exc:
        raise _error("DATASET001_UNVERIFIED", "DATASET-001 artifacts failed verification") from exc
    dataset: Mapping[str, Any] | None = (
        manifest.get("dataset") if isinstance(manifest, Mapping) else None
    )
    if not isinstance(dataset, Mapping) or dataset.get("dataset_id") != _EXPECTED_DATASET_ID:
        raise _error("DATASET001_INVALID", "corpus is not DATASET-001")
    version = dataset.get("version")
    dimensions = dataset.get("dimensions")
    if not isinstance(version, str) or not version:
        raise _error("DATASET001_INVALID", "corpus version is invalid")
    if isinstance(dimensions, bool) or not isinstance(dimensions, int) or dimensions <= 0:
        raise _error("DATASET001_INVALID", "corpus dimensions are invalid")
    generation_manifest_sha256 = sha256_file(directory / "generation_manifest.json")
    return PinnedDatasetIdentity(
        data_identity=f"{version}:sha256:{generation_manifest_sha256}",
        generation_manifest_sha256=generation_manifest_sha256,
        base_vectors_sha256=sha256_file(directory / "base_vectors.npy"),
        dimensions=dimensions,
        version=version,
    )


class Exp010V2HostComposition:
    """The offline production composition structure for the real v2 path."""

    def __init__(
        self,
        *,
        root: Path,
        dataset001_dir: Path,
        stream_id: str,
        metric: Metric,
        threshold_stratum: str,
        configuration_identity: str,
        flat_binding_id: str,
        hnsw_binding_id: str,
        source_revision: str,
        environment_manifest_sha256: str,
        serving_executor: RangeServingExecutor,
        shadow_capture_executor: V2ShadowCaptureExecutor,
        detector_seed: int,
        clock,
        shadow_captured_at_clock,
        data_identity: object = None,
    ) -> None:
        # ADR-014 item 10: refuse a caller-supplied identity outright rather
        # than validating one, so there is no path that accepts a literal.
        if data_identity is not None:
            raise _error(
                "DATA_IDENTITY_NOT_DERIVED",
                "data_identity is derived from the verified DATASET-001 corpus "
                "and may not be supplied by a caller",
            )
        self.dataset_identity = pin_dataset001_identity(dataset001_dir)
        self.stream_key = MonitorStreamKey(
            stream_id,
            metric,
            threshold_stratum,
            configuration_identity,
            self.dataset_identity.data_identity,
            flat_binding_id,
            hnsw_binding_id,
        )
        directory = Path(root)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._closed = False
        self._stores: list[object] = []
        try:
            self.response_store = SQLiteHostResponseCommitStore(
                directory / "v2_source.sqlite3",
                stream_key=self.stream_key,
                source_revision=source_revision,
                environment_manifest_sha256=environment_manifest_sha256,
            )
            self._stores.append(self.response_store)
            self.host = ReferenceV2Host(
                serving_executor=serving_executor,
                response_store=self.response_store,
                clock=clock,
            )
            # Two independent cursors: the shadow worker and EXP-010 capture
            # never consume, delete, or advance each other's evidence.
            self.shadow_source = V2GenuineWorkloadObservationSource(
                store=self.response_store,
                consumer_id=SHADOW_CONSUMER_ID,
                clock=clock,
            )
            self.shadow_attempt_store = SQLiteShadowAttemptStore(
                directory / "v2_shadow_attempts.sqlite3",
                stream_key=self.stream_key,
                source_revision=source_revision,
                environment_manifest_sha256=environment_manifest_sha256,
            )
            self._stores.append(self.shadow_attempt_store)
            self.shadow_worker = V2ShadowWorker(
                capture_executor=shadow_capture_executor,
                captured_at_clock=shadow_captured_at_clock,
                attempt_store=self.shadow_attempt_store,
            )
            self.detector_store = SQLiteHostWindowDetectorV2Store(
                directory / "v2_detector.sqlite3", stream_key=self.stream_key
            )
            self._stores.append(self.detector_store)
            self.attestation_store = SQLiteRealDetectorAttestationStore(
                directory / "v2_attestation.sqlite3", stream_key=self.stream_key
            )
            self._stores.append(self.attestation_store)
            self.finalization_store = SQLiteWindowFinalizationStore(
                directory / "v2_window_finalization.sqlite3",
                stream_key=self.stream_key,
                source_revision=source_revision,
                environment_manifest_sha256=environment_manifest_sha256,
            )
            self._stores.append(self.finalization_store)
        except Exception:
            self.close()
            raise
        self.evaluator = GovernedV2DetectorEvaluator(
            previous_evidence_source=self.attestation_store,
            detector_seed=detector_seed,
            source_revision=source_revision,
            environment_manifest_sha256=environment_manifest_sha256,
        )
        self.source_revision = source_revision
        self.environment_manifest_sha256 = environment_manifest_sha256

    @property
    def data_identity(self) -> str:
        return self.dataset_identity.data_identity

    def execute(self, request: RangeQueryRequest) -> V2VisibleResponse:
        """Serve one request through the v2 host; ADR-007's `offer()` is untouched."""

        return self.host.execute(request)

    def exp010_source(
        self, *, start_source_sequence: int
    ) -> V2GenuineWorkloadObservationSource:
        """Open the independent EXP-010 cursor at an exact source sequence."""

        return V2GenuineWorkloadObservationSource(
            store=self.response_store,
            consumer_id=EXP010_CONSUMER_ID,
            clock=self.host._clock,  # noqa: SLF001 - same composition owns both
            start_source_sequence=start_source_sequence,
        )

    def verified_real_latest(self) -> VerifiedRealDetectorHead | None:
        """Return the current real head, or None when none is real-eligible."""

        return self.attestation_store.load_verified_real_latest(self.detector_store)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for store in reversed(self._stores):
            try:
                store.close()
            except Exception:
                pass

    def __enter__(self) -> "Exp010V2HostComposition":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
