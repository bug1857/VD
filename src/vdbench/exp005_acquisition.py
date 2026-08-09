"""Read-only EXP-005 shadow acquisition with reviewed identity baselines.

``snapshot-identities`` is a bootstrap operation that reads live index metadata
and writes a candidate immutable baseline for human review.  ``capture`` refuses
to trust first-seen metadata: it requires that reviewed baseline, issues only
shadow searches, and persists twelve source traces for one metric/stratum.
Neither path creates collections or calls any canary or restore operation.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import subprocess
from typing import Any, Protocol

import numpy as np

from .actuation import QueryId, ShadowActuationContext, ShadowResult
from .artifacts import canonical_json_bytes, sha256_file, verify_dataset_artifacts, write_immutable_json
from .config import ENV001_PINS, IndexTrack, Metric, THRESHOLD_LABELS
from .milvus import CollectionIdentity, INDEX_NAME, MilvusHarness
from .milvus_actuation import (
    ActuationWorkload,
    CanaryBounds,
    CanaryBoundEstimatorLike,
    CollectionIdentityBinding,
    MilvusActuationClient,
    ShadowAuditTrace,
    ShadowAuditTraceSinkLike,
    StackHealth,
    StackHealthProbeLike,
)
from .runner import load_dataset
from .shadow_artifacts import load_persisted_shadow_trace_envelope, persist_shadow_trace_envelope
from .shadow_window import PersistedShadowTraceEnvelope, assemble_shadow_window, hash_shadow_audit_trace


IDENTITY_BASELINE_SCHEMA_VERSION = "exp005-identity-baseline-v1"
CAPTURE_MANIFEST_SCHEMA_VERSION = "exp005-live-shadow-capture-v1"
TRACE_COUNT_PER_WINDOW = 4
TRACE_SIZE = 50
WINDOW_QUERY_COUNT = TRACE_COUNT_PER_WINDOW * TRACE_SIZE
WINDOW_ROLES = ("reference", "current-1", "current-2")


class Exp005AcquisitionError(ValueError):
    """Raised before or during a capture when evidence cannot be trusted."""


@dataclass(frozen=True, slots=True)
class IdentityBaseline:
    """Reviewed expected identities for exactly one EXP-005 configuration."""

    metric: Metric
    threshold_stratum: str
    candidate_ef: int
    last_known_good_ef: int
    configuration_identity: str
    data_identity: str
    flat_binding: CollectionIdentityBinding
    hnsw_binding: CollectionIdentityBinding
    sha256: str


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """Immutable summary of one persisted three-window shadow acquisition."""

    output_dir: Path
    manifest_path: Path
    completion_path: Path
    trace_paths_by_role: Mapping[str, tuple[Path, ...]]

    @property
    def trace_paths(self) -> tuple[Path, ...]:
        return tuple(path for role in WINDOW_ROLES for path in self.trace_paths_by_role[role])


class _ShadowAdapterLike(Protocol):
    shadow_trace_sink: ShadowAuditTraceSinkLike | None

    def shadow_candidate(
        self,
        *,
        context: ShadowActuationContext,
        candidate_ef: int,
        last_known_good_ef: int,
    ) -> ShadowResult: ...


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise Exp005AcquisitionError("NONFINITE_JSON_VALUE")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise Exp005AcquisitionError("NON_STRING_JSON_KEY")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise Exp005AcquisitionError(f"UNSUPPORTED_JSON_VALUE:{type(value).__name__}")


def _identity_payload(identity: CollectionIdentity) -> dict[str, object]:
    if not isinstance(identity, CollectionIdentity):
        raise Exp005AcquisitionError("IDENTITY_INVALID")
    return {
        "collection_name": identity.collection_name,
        "metric": identity.metric,
        "index_track": identity.index_track,
        "description": _json_value(identity.description),
    }


def _binding_id(identity: CollectionIdentity) -> str:
    digest = hashlib.sha256(canonical_json_bytes(_identity_payload(identity))).hexdigest()
    return f"exp005-index-binding-v1:{digest}"


def _baseline_payload_without_digest(baseline: IdentityBaseline) -> dict[str, object]:
    return {
        "schema_version": IDENTITY_BASELINE_SCHEMA_VERSION,
        "metric": baseline.metric.value,
        "threshold_stratum": baseline.threshold_stratum,
        "candidate_ef": baseline.candidate_ef,
        "last_known_good_ef": baseline.last_known_good_ef,
        "configuration_identity": baseline.configuration_identity,
        "data_identity": baseline.data_identity,
        "flat_binding": {
            "identity_id": baseline.flat_binding.identity_id,
            "expected": _identity_payload(baseline.flat_binding.expected),
        },
        "hnsw_binding": {
            "identity_id": baseline.hnsw_binding.identity_id,
            "expected": _identity_payload(baseline.hnsw_binding.expected),
        },
    }


def _with_baseline_digest(payload: Mapping[str, object]) -> dict[str, object]:
    return {
        "expected_baseline_sha256": hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
        "baseline": dict(payload),
    }


def _identity_from_payload(value: object, *, expected_track: IndexTrack) -> CollectionIdentity:
    if not isinstance(value, dict) or frozenset(value) != {
        "collection_name", "metric", "index_track", "description"
    }:
        raise Exp005AcquisitionError("BASELINE_SCHEMA_MISMATCH")
    collection_name = value["collection_name"]
    metric = value["metric"]
    index_track = value["index_track"]
    if (
        not isinstance(collection_name, str)
        or not collection_name
        or not isinstance(metric, str)
        or not metric
        or index_track != expected_track.value
    ):
        raise Exp005AcquisitionError("BASELINE_IDENTITY_INVALID")
    try:
        description = _json_value(value["description"])
    except Exp005AcquisitionError:
        raise
    return CollectionIdentity(collection_name, metric, index_track, description)


def _baseline_from_payload(payload: object) -> IdentityBaseline:
    required = {
        "schema_version", "metric", "threshold_stratum", "candidate_ef",
        "last_known_good_ef", "configuration_identity", "data_identity",
        "flat_binding", "hnsw_binding",
    }
    if not isinstance(payload, dict) or frozenset(payload) != required:
        raise Exp005AcquisitionError("BASELINE_SCHEMA_MISMATCH")
    if payload["schema_version"] != IDENTITY_BASELINE_SCHEMA_VERSION:
        raise Exp005AcquisitionError("BASELINE_SCHEMA_MISMATCH")
    try:
        metric = Metric(payload["metric"])
    except (TypeError, ValueError) as exc:
        raise Exp005AcquisitionError("BASELINE_METRIC_INVALID") from exc
    stratum = payload["threshold_stratum"]
    candidate = payload["candidate_ef"]
    lkg = payload["last_known_good_ef"]
    identities = (payload["configuration_identity"], payload["data_identity"])
    if (
        stratum not in THRESHOLD_LABELS
        or isinstance(candidate, bool) or candidate not in {200, 400, 800, 1600}
        or isinstance(lkg, bool) or lkg not in {200, 400, 800, 1600}
        or not all(isinstance(value, str) and value for value in identities)
    ):
        raise Exp005AcquisitionError("BASELINE_CONFIGURATION_INVALID")

    def binding(value: object, track: IndexTrack) -> CollectionIdentityBinding:
        if not isinstance(value, dict) or frozenset(value) != {"identity_id", "expected"}:
            raise Exp005AcquisitionError("BASELINE_SCHEMA_MISMATCH")
        identity_id = value["identity_id"]
        if not isinstance(identity_id, str) or not identity_id:
            raise Exp005AcquisitionError("BASELINE_BINDING_INVALID")
        expected = _identity_from_payload(value["expected"], expected_track=track)
        if expected.metric != metric.value:
            raise Exp005AcquisitionError("BASELINE_IDENTITY_METRIC_MISMATCH")
        if identity_id != _binding_id(expected):
            raise Exp005AcquisitionError("BASELINE_BINDING_DIGEST_MISMATCH")
        return CollectionIdentityBinding(identity_id, expected)

    digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return IdentityBaseline(
        metric=metric,
        threshold_stratum=stratum,
        candidate_ef=candidate,
        last_known_good_ef=lkg,
        configuration_identity=identities[0],
        data_identity=identities[1],
        flat_binding=binding(payload["flat_binding"], IndexTrack.FLAT),
        hnsw_binding=binding(payload["hnsw_binding"], IndexTrack.HNSW),
        sha256=digest,
    )


def capture_identity_baseline(
    *,
    client: object,
    metric: Metric,
    threshold_stratum: str,
    candidate_ef: int,
    last_known_good_ef: int,
    flat_collection_name: str,
    hnsw_collection_name: str,
    configuration_identity: str,
    data_identity: str,
) -> IdentityBaseline:
    """Read live index descriptions once for human review; never writes Milvus."""

    if threshold_stratum not in THRESHOLD_LABELS or candidate_ef not in {200, 400, 800, 1600} or last_known_good_ef not in {200, 400, 800, 1600}:
        raise Exp005AcquisitionError("BASELINE_CONFIGURATION_INVALID")
    if not all(isinstance(value, str) and value for value in (flat_collection_name, hnsw_collection_name, configuration_identity, data_identity)):
        raise Exp005AcquisitionError("BASELINE_IDENTITY_INVALID")
    harness = MilvusHarness(client, dimensions=1)  # index identity does not use dimensions
    flat = harness.index_identity(flat_collection_name, metric, IndexTrack.FLAT)
    hnsw = harness.index_identity(hnsw_collection_name, metric, IndexTrack.HNSW)
    provisional = IdentityBaseline(
        metric=metric,
        threshold_stratum=threshold_stratum,
        candidate_ef=candidate_ef,
        last_known_good_ef=last_known_good_ef,
        configuration_identity=configuration_identity,
        data_identity=data_identity,
        flat_binding=CollectionIdentityBinding(_binding_id(flat), flat),
        hnsw_binding=CollectionIdentityBinding(_binding_id(hnsw), hnsw),
        sha256="",
    )
    payload = _baseline_payload_without_digest(provisional)
    return replace(
        provisional,
        sha256=hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
    )


def persist_identity_baseline(path: Path, baseline: IdentityBaseline) -> None:
    """Publish a human-reviewable immutable baseline without replacement."""

    payload = _baseline_payload_without_digest(baseline)
    if baseline.sha256 != hashlib.sha256(canonical_json_bytes(payload)).hexdigest():
        raise Exp005AcquisitionError("BASELINE_DIGEST_MISMATCH")
    write_immutable_json(path, _with_baseline_digest(payload))


def load_identity_baseline(path: Path) -> IdentityBaseline:
    """Load a baseline only after strict schema and digest validation."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Exp005AcquisitionError("BASELINE_MALFORMED") from exc
    if not isinstance(document, dict) or frozenset(document) != {"expected_baseline_sha256", "baseline"}:
        raise Exp005AcquisitionError("BASELINE_SCHEMA_MISMATCH")
    expected = document["expected_baseline_sha256"]
    if not isinstance(expected, str) or len(expected) != 64:
        raise Exp005AcquisitionError("BASELINE_DIGEST_INVALID")
    baseline = _baseline_from_payload(document["baseline"])
    if baseline.sha256 != expected:
        raise Exp005AcquisitionError("BASELINE_DIGEST_MISMATCH")
    return baseline


class _StrictUtcClock:
    def __init__(self) -> None:
        self._last: datetime | None = None

    def __call__(self) -> str:
        now = datetime.now(timezone.utc)
        if self._last is not None and now <= self._last:
            now = self._last + timedelta(microseconds=1)
        self._last = now
        return now.isoformat(timespec="microseconds").replace("+00:00", "Z")


class _TraceSink:
    def __init__(self, *, path: Path, trace_id: str, sequence_index: int, clock: _StrictUtcClock) -> None:
        self.path = path
        self.trace_id = trace_id
        self.sequence_index = sequence_index
        self.clock = clock
        self.called = False

    def append(self, trace: ShadowAuditTrace) -> None:
        if self.called:
            raise Exp005AcquisitionError("TRACE_SINK_CALLED_MORE_THAN_ONCE")
        self.called = True
        envelope = PersistedShadowTraceEnvelope(
            trace_id=self.trace_id,
            captured_at_utc=self.clock(),
            sequence_index=self.sequence_index,
            declared_observation_count=len(trace.queries),
            expected_trace_sha256=hash_shadow_audit_trace(trace),
            trace=trace,
        )
        persist_shadow_trace_envelope(self.path, envelope)


def _partition_query_ids(values: Sequence[QueryId]) -> tuple[tuple[QueryId, ...], ...]:
    identifiers = tuple(values)
    if len(identifiers) != WINDOW_QUERY_COUNT or len(set(identifiers)) != WINDOW_QUERY_COUNT:
        raise Exp005AcquisitionError("QUERY_IDS_MUST_BE_EXACTLY_200_UNIQUE")
    if any(isinstance(value, bool) or not isinstance(value, (int, str)) for value in identifiers):
        raise Exp005AcquisitionError("QUERY_IDS_INVALID")
    return tuple(
        identifiers[index * TRACE_SIZE : (index + 1) * TRACE_SIZE]
        for index in range(TRACE_COUNT_PER_WINDOW)
    )


def _context(
    baseline: IdentityBaseline,
    query_ids: tuple[QueryId, ...],
    timestamp: str,
) -> ShadowActuationContext:
    return ShadowActuationContext(
        metric=baseline.metric,
        threshold_stratum=baseline.threshold_stratum,
        collection_name=baseline.hnsw_binding.expected.collection_name,
        configuration_identity=baseline.configuration_identity,
        index_identity=baseline.hnsw_binding.identity_id,
        flat_index_identity=baseline.flat_binding.identity_id,
        data_identity=baseline.data_identity,
        audited_query_ids=query_ids,
        occurred_at_utc=timestamp,
    )


def capture_stationary_replay(
    *,
    adapter: _ShadowAdapterLike,
    baseline: IdentityBaseline,
    measured_query_ids: Sequence[QueryId],
    output_dir: Path,
    capture_id: str,
    preflight_evidence: Mapping[str, object] | None = None,
) -> CaptureResult:
    """Collect exactly three four-trace windows through shadow-only calls.

    The same 200 measured query IDs are partitioned deterministically into four
    ordered 50-query groups for every role.  They may repeat across the three
    stationary windows; uniqueness is intentionally per assembled window.
    """

    if not isinstance(capture_id, str) or not capture_id:
        raise Exp005AcquisitionError("CAPTURE_ID_INVALID")
    groups = _partition_query_ids(measured_query_ids)
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite capture directory: {output_dir}")
    output_dir.mkdir(parents=True)
    trace_dir = output_dir / "traces"
    trace_dir.mkdir()
    manifest_path = output_dir / "capture_manifest.json"
    write_immutable_json(
        manifest_path,
        {
            "schema_version": CAPTURE_MANIFEST_SCHEMA_VERSION,
            "capture_id": capture_id,
            "baseline_sha256": baseline.sha256,
            "metric": baseline.metric.value,
            "threshold_stratum": baseline.threshold_stratum,
            "candidate_ef": baseline.candidate_ef,
            "last_known_good_ef": baseline.last_known_good_ef,
            "roles": list(WINDOW_ROLES),
            "trace_size": TRACE_SIZE,
            "window_query_count": WINDOW_QUERY_COUNT,
        },
    )
    if preflight_evidence is not None:
        write_immutable_json(output_dir / "live_preflight.json", dict(preflight_evidence))
    clock = _StrictUtcClock()
    paths_by_role: dict[str, tuple[Path, ...]] = {}
    original_sink = adapter.shadow_trace_sink
    try:
        for role in WINDOW_ROLES:
            paths: list[Path] = []
            for sequence_index, group in enumerate(groups):
                path = trace_dir / f"{role}-{sequence_index}.json"
                sink = _TraceSink(
                    path=path,
                    trace_id=f"{capture_id}:{role}:{sequence_index}",
                    sequence_index=sequence_index,
                    clock=clock,
                )
                adapter.shadow_trace_sink = sink
                result = adapter.shadow_candidate(
                    context=_context(baseline, group, clock()),
                    candidate_ef=baseline.candidate_ef,
                    last_known_good_ef=baseline.last_known_good_ef,
                )
                if not sink.called:
                    raise Exp005AcquisitionError("TRACE_SINK_NOT_CALLED")
                if not isinstance(result, ShadowResult) or not result.success:
                    raise Exp005AcquisitionError("SHADOW_RESULT_FAILED")
                paths.append(path)
            paths_by_role[role] = tuple(paths)
    finally:
        adapter.shadow_trace_sink = original_sink

    completed_windows: dict[str, object] = {}
    completed_at: list[str] = []
    for role, paths in paths_by_role.items():
        envelopes = tuple(load_persisted_shadow_trace_envelope(path) for path in paths)
        window = assemble_shadow_window(window_id=f"{capture_id}:{role}", envelopes=envelopes)
        if not window.complete or window.manifest_sha256 is None:
            raise Exp005AcquisitionError(f"ASSEMBLED_WINDOW_INVALID:{role}:{','.join(window.reason_codes)}")
        completed_at.append(envelopes[-1].captured_at_utc)
        completed_windows[role] = {
            "window_id": window.window_id,
            "manifest_sha256": window.manifest_sha256,
            "trace_payload_sha256": [envelope.expected_trace_sha256 for envelope in envelopes],
        }
    if not completed_at[0] < completed_at[1] < completed_at[2]:
        raise Exp005AcquisitionError("WINDOW_CHRONOLOGY_INVALID")
    completion_path = output_dir / "capture_completion.json"
    write_immutable_json(
        completion_path,
        {
            "schema_version": CAPTURE_MANIFEST_SCHEMA_VERSION,
            "capture_id": capture_id,
            "baseline_sha256": baseline.sha256,
            "windows": completed_windows,
            "no_actuation": {
                "start_canary_called": False,
                "restore_last_known_good_called": False,
                "rollback_called": False,
                "collection_create_called": False,
                "collection_mutation_called": False,
            },
        },
    )
    return CaptureResult(output_dir, manifest_path, completion_path, paths_by_role)


class _NoCanaryBoundEstimator(CanaryBoundEstimatorLike):
    """Makes an accidental canary path fail instead of fabricating bounds."""

    def estimate(self, measurements: object) -> CanaryBounds:
        raise AssertionError("EXP-005 read-only capture must not estimate canary bounds")


class DockerHealthProbe(StackHealthProbeLike):
    """Read Docker's existing etcd/MinIO health states without mutation."""

    def __init__(self, *, etcd_container: str, minio_container: str) -> None:
        self.etcd_container = etcd_container
        self.minio_container = minio_container

    @staticmethod
    def _status(container: str) -> tuple[bool, str]:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            check=False,
            capture_output=True,
            text=True,
        )
        value = result.stdout.strip()
        detail = value if value else result.stderr.strip() or f"exit={result.returncode}"
        return result.returncode == 0 and value == "healthy", f"{container}={detail}"

    def check(self) -> StackHealth:
        etcd_ok, etcd_detail = self._status(self.etcd_container)
        minio_ok, minio_detail = self._status(self.minio_container)
        return StackHealth(etcd_ok, minio_ok, f"{etcd_detail}; {minio_detail}")


def _threshold_radius(dataset_dir: Path, metric: Metric, stratum: str) -> float:
    try:
        values = json.loads((dataset_dir / "thresholds.json").read_text(encoding="utf-8"))
        entries = values[metric.value]
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise Exp005AcquisitionError("THRESHOLD_ARTIFACT_MALFORMED") from exc
    for entry in entries:
        if isinstance(entry, dict) and entry.get("label") == stratum:
            radius = entry.get("radius")
            if isinstance(radius, (int, float)) and not isinstance(radius, bool) and math.isfinite(radius):
                return float(radius)
    raise Exp005AcquisitionError("THRESHOLD_STRATUM_NOT_FOUND")


def _derived_identities(
    *, dataset_dir: Path, metric: Metric, stratum: str, candidate_ef: int, lkg_ef: int
) -> tuple[str, str, float]:
    manifest = verify_dataset_artifacts(dataset_dir)
    if not isinstance(manifest, dict):
        raise Exp005AcquisitionError("DATASET_MANIFEST_INVALID")
    manifest_sha256 = sha256_file(dataset_dir / "generation_manifest.json")
    radius = _threshold_radius(dataset_dir, metric, stratum)
    data_identity = f"DATASET-001-v1:sha256:{manifest_sha256}"
    configuration_payload = {
        "schema_version": "exp005-shadow-configuration-v1",
        "dataset_manifest_sha256": manifest_sha256,
        "metric": metric.value,
        "threshold_stratum": stratum,
        "radius": radius,
        "range_filter": 0.0 if metric is Metric.L2 else 1.0,
        "limit": 100,
        "candidate_ef": candidate_ef,
        "last_known_good_ef": lkg_ef,
        "sentinel_ef": 100,
    }
    configuration_identity = "exp005-config-v1:sha256:" + hashlib.sha256(
        canonical_json_bytes(configuration_payload)
    ).hexdigest()
    return configuration_identity, data_identity, radius


def derive_exp005_identities(
    *,
    dataset_dir: Path,
    metric: Metric,
    stratum: str,
    candidate_ef: int,
    lkg_ef: int,
) -> tuple[str, str, float]:
    """Derive the reviewed EXP-005 lineage tuple without contacting Milvus.

    EXP-008 reuses this canonical identity function instead of copying the
    configuration-hash recipe, so a baseline remains bound to exactly the same
    DATASET-001/range-query semantics across both experiments.
    """

    return _derived_identities(
        dataset_dir=dataset_dir,
        metric=metric,
        stratum=stratum,
        candidate_ef=candidate_ef,
        lkg_ef=lkg_ef,
    )


def _preflight_live_adapter(
    adapter: MilvusActuationClient, baseline: IdentityBaseline
) -> dict[str, object]:
    """Read health/load/identity evidence before or after a capture, fail closed."""

    health = adapter.stack_health_probe.check()
    if not health.etcd_healthy or not health.minio_healthy:
        raise Exp005AcquisitionError(f"STACK_HEALTH_FAILED:{health.detail}")
    evidence: dict[str, object] = {"stack_health": health.detail, "tracks": {}}
    for track, binding in (
        (IndexTrack.FLAT, baseline.flat_binding),
        (IndexTrack.HNSW, baseline.hnsw_binding),
    ):
        name = binding.expected.collection_name
        try:
            state = adapter.client.get_load_state(collection_name=name)
            raw_state = state.get("state") if isinstance(state, dict) else state
            loaded = getattr(raw_state, "name", str(raw_state)) == "Loaded"
            actual = adapter.harness.index_identity(name, baseline.metric, track)
        except Exception as exc:  # noqa: BLE001 - external client boundary
            raise Exp005AcquisitionError(f"PREFLIGHT_READ_FAILED:{track.value}:{type(exc).__name__}") from exc
        matches = binding.matches(actual)
        evidence["tracks"][track.value] = {
            "collection_name": name,
            "loaded": loaded,
            "binding_match": matches,
            "identity": _identity_payload(actual),
        }
        if not loaded or not matches:
            raise Exp005AcquisitionError(f"PREFLIGHT_IDENTITY_OR_LOAD_FAILED:{track.value}")
    return evidence


def capture_live_from_uri(
    *,
    uri: str,
    dataset_dir: Path,
    baseline_path: Path,
    output_dir: Path,
    capture_id: str,
    detector_seed: int,
    etcd_container: str = "milvus-etcd",
    minio_container: str = "milvus-minio",
) -> CaptureResult:
    """Explicit real-Milvus read-only capture; no collection setup or actuation."""

    baseline = load_identity_baseline(baseline_path)
    expected_config, expected_data, radius = _derived_identities(
        dataset_dir=dataset_dir,
        metric=baseline.metric,
        stratum=baseline.threshold_stratum,
        candidate_ef=baseline.candidate_ef,
        lkg_ef=baseline.last_known_good_ef,
    )
    if (
        baseline.configuration_identity != expected_config
        or baseline.data_identity != expected_data
    ):
        raise Exp005AcquisitionError("BASELINE_DATASET_CONFIGURATION_MISMATCH")
    bundle, _, _ = load_dataset(dataset_dir)
    query_vectors = {
        index: vector for index, vector in enumerate(bundle.measured_queries)
    }
    workload = ActuationWorkload(
        query_vectors=query_vectors,
        base_ids=bundle.ids,
        base_vectors=bundle.base_vectors,
        threshold_radii={(baseline.metric, baseline.threshold_stratum): radius},
        collection_names={
            (baseline.metric, IndexTrack.FLAT): baseline.flat_binding.expected.collection_name,
            (baseline.metric, IndexTrack.HNSW): baseline.hnsw_binding.expected.collection_name,
        },
        identity_bindings={
            (baseline.metric, IndexTrack.FLAT): baseline.flat_binding,
            (baseline.metric, IndexTrack.HNSW): baseline.hnsw_binding,
        },
        configuration_identity=baseline.configuration_identity,
        data_identity=baseline.data_identity,
    )
    adapter = MilvusActuationClient.from_uri(
        uri,
        workload=workload,
        routing_seed=detector_seed,
        bound_estimator=_NoCanaryBoundEstimator(),
        stack_health_probe=DockerHealthProbe(
            etcd_container=etcd_container, minio_container=minio_container
        ),
        initial_ef=baseline.last_known_good_ef,
    )
    preflight = _preflight_live_adapter(adapter, baseline)
    result = capture_stationary_replay(
        adapter=adapter,
        baseline=baseline,
        measured_query_ids=tuple(range(bundle.measured_queries.shape[0])),
        output_dir=output_dir,
        capture_id=capture_id,
        preflight_evidence=preflight,
    )
    postflight = _preflight_live_adapter(adapter, baseline)
    write_immutable_json(result.output_dir / "live_postflight.json", postflight)
    return result


def _live_client(uri: str) -> object:
    """Lazy PyMilvus construction used only by explicit CLI commands."""

    from pymilvus import MilvusClient

    return MilvusClient(uri=uri)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vd-exp005-shadow")
    commands = parser.add_subparsers(dest="command", required=True)
    snapshot = commands.add_parser("snapshot-identities", help="create a reviewable read-only identity baseline")
    snapshot.add_argument("--uri", required=True)
    snapshot.add_argument("--dataset-dir", type=Path, required=True)
    snapshot.add_argument("--metric", choices=[metric.value for metric in Metric], required=True)
    snapshot.add_argument("--threshold-stratum", choices=sorted(THRESHOLD_LABELS), required=True)
    snapshot.add_argument("--candidate-ef", type=int, required=True)
    snapshot.add_argument("--last-known-good-ef", type=int, required=True)
    snapshot.add_argument("--flat-collection", required=True)
    snapshot.add_argument("--hnsw-collection", required=True)
    snapshot.add_argument("--output", type=Path, required=True)
    capture = commands.add_parser("capture", help="capture twelve read-only EXP-005 shadow traces")
    capture.add_argument("--uri", required=True)
    capture.add_argument("--dataset-dir", type=Path, required=True)
    capture.add_argument("--baseline", type=Path, required=True)
    capture.add_argument("--output-dir", type=Path, required=True)
    capture.add_argument("--capture-id", required=True)
    capture.add_argument("--detector-seed", type=int, required=True)
    capture.add_argument("--etcd-container", default="milvus-etcd")
    capture.add_argument("--minio-container", default="milvus-minio")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "snapshot-identities":
        metric = Metric(args.metric)
        configuration_identity, data_identity, _ = _derived_identities(
            dataset_dir=args.dataset_dir,
            metric=metric,
            stratum=args.threshold_stratum,
            candidate_ef=args.candidate_ef,
            lkg_ef=args.last_known_good_ef,
        )
        baseline = capture_identity_baseline(
            client=_live_client(args.uri),
            metric=metric,
            threshold_stratum=args.threshold_stratum,
            candidate_ef=args.candidate_ef,
            last_known_good_ef=args.last_known_good_ef,
            flat_collection_name=args.flat_collection,
            hnsw_collection_name=args.hnsw_collection,
            configuration_identity=configuration_identity,
            data_identity=data_identity,
        )
        persist_identity_baseline(args.output, baseline)
        print(json.dumps(_with_baseline_digest(_baseline_payload_without_digest(baseline)), sort_keys=True))
        return 0
    result = capture_live_from_uri(
        uri=args.uri,
        dataset_dir=args.dataset_dir,
        baseline_path=args.baseline,
        output_dir=args.output_dir,
        capture_id=args.capture_id,
        detector_seed=args.detector_seed,
        etcd_container=args.etcd_container,
        minio_container=args.minio_container,
    )
    print(json.dumps({"capture_dir": str(result.output_dir), "trace_count": len(result.trace_paths)}, sort_keys=True))
    return 0


__all__ = [
    "CAPTURE_MANIFEST_SCHEMA_VERSION",
    "IDENTITY_BASELINE_SCHEMA_VERSION",
    "CaptureResult",
    "DockerHealthProbe",
    "Exp005AcquisitionError",
    "IdentityBaseline",
    "capture_identity_baseline",
    "capture_live_from_uri",
    "capture_stationary_replay",
    "load_identity_baseline",
    "persist_identity_baseline",
]


if __name__ == "__main__":
    raise SystemExit(main())
