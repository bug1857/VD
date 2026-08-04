"""Fail-closed, read-only ENV-001 preflight evidence for EXP-009 Stage 4.

Purpose:
    Capture the exact health, collection-load, and FLAT/HNSW identity facts
    needed by the Stage-4 runtime probe before a human-gated canary can even be
    considered.  This module deliberately has no PyMilvus import or client
    factory: callers inject a client behind a two-method read-only facade.
Inputs:
    A reviewed EXP-005 identity baseline, verified DATASET-001 provenance,
    dependency-injected client and stack-health probe, immutable output path,
    and an injected RFC3339-UTC clock.
Outputs:
    A no-replacement evidence directory containing canonical result, manifest,
    and receipt documents plus a small immutable capture result.
Complexity:
    A successful capture makes exactly four ``get_load_state`` and eight
    ``describe_index`` calls; it makes no vector search or write call.
Failure modes:
    Invalid inputs, unavailable health/load/identity facts, baseline mismatch,
    unsafe runtime state, unexpected call transcript, and output-integrity
    failures fail closed.  An incomplete capture is persisted when possible;
    an existing output path is refused before any client operation.
Extension point:
    A CLI may lazily construct a PyMilvus client outside this module.  Candidate
    routing, grants, policy, search, and configuration mutation are not an
    extension point here.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
from types import MappingProxyType
from typing import Protocol

from .artifacts import canonical_json_bytes, git_state, sha256_file
from .canary_route_state import RouteStateBinding
from .canary_runtime_probe import Stage4ServingRuntimeProbe
from .config import INDEX_NAME, THRESHOLD_LABELS, IndexTrack, Metric
from .exp005_acquisition import IdentityBaseline, load_identity_baseline
from .milvus import CollectionIdentity, MilvusHarness
from .milvus_serving import HostServingPlan, MilvusRangeServingExecutor
from .runner import load_dataset
from .shadow_event_types import MonitorStreamKey


__all__ = [
    "PreflightCaptureResult",
    "PreflightEvidenceTarget",
    "ReadOnlyPreflightClient",
    "capture_read_only_preflight",
    "target_from_artifacts",
    "verify_preflight_evidence",
]


_SCHEMA_VERSION = "exp009-stage4-runtime-preflight-v1"
_MANIFEST_SCHEMA_VERSION = "exp009-stage4-runtime-preflight-manifest-v1"
_RECEIPT_SCHEMA_VERSION = "exp009-stage4-runtime-preflight-receipt-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_UTC = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_STREAM_ID = "exp009-stage4-runtime-l2-target-075"
_SUCCESS_COUNTS = {"get_load_state": 4, "describe_index": 8}
_EFFECT_ASSERTION = {
    "issues_search": False,
    "issues_insert": False,
    "issues_collection_or_index_mutation": False,
    "uses_grant_or_candidate_route": False,
    "uses_configuration_mutation": False,
    "uses_only_read_only_client_methods": True,
}
_RESULT_FIELDS = frozenset(
    {
        "schema_version", "status", "captured_at_utc", "git", "target",
        "pre_identity", "post_identity", "readiness", "slot_safety",
        "client_calls", "call_counts", "reason_codes", "no_effect_assertion",
        "self_sha256",
    }
)
_MANIFEST_FIELDS = frozenset(
    {"schema_version", "status", "result_file_sha256", "result_self_sha256", "self_sha256"}
)
_RECEIPT_FIELDS = frozenset(
    {"schema_version", "status", "result_self_sha256", "manifest_self_sha256", "self_sha256"}
)
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class ReadOnlyMilvusClientLike(Protocol):
    """Only client operations permitted by the runtime-preflight contract."""

    def get_load_state(self, *, collection_name: str) -> object: ...

    def describe_index(self, *, collection_name: str, index_name: str) -> object: ...


class StackHealthProbeLike(Protocol):
    """Structural stack probe kept outside the client facade."""

    def check(self) -> object: ...


@dataclass(frozen=True, slots=True)
class PreflightEvidenceTarget:
    """Frozen one-stream provenance and semantic inputs for this preflight."""

    baseline: IdentityBaseline
    dataset_manifest_sha256: str
    dimensions: int
    threshold_radius: float

    def __post_init__(self) -> None:
        if not isinstance(self.baseline, IdentityBaseline):
            raise TypeError("baseline must be an IdentityBaseline")
        if (
            self.baseline.metric is not Metric.L2
            or self.baseline.threshold_stratum != "target-075"
            or self.baseline.candidate_ef != 800
            or self.baseline.last_known_good_ef != 400
        ):
            raise ValueError("target must use the frozen L2 target-075 400-to-800 baseline")
        for name, value in (
            ("baseline.sha256", self.baseline.sha256),
            ("dataset_manifest_sha256", self.dataset_manifest_sha256),
        ):
            if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
                raise ValueError(f"{name} must be a lower-case SHA-256 digest")
        if isinstance(self.dimensions, bool) or not isinstance(self.dimensions, int) or self.dimensions <= 0:
            raise ValueError("dimensions must be a positive integer")
        if (
            not isinstance(self.threshold_radius, (int, float))
            or isinstance(self.threshold_radius, bool)
            or not math.isfinite(float(self.threshold_radius))
            or float(self.threshold_radius) <= 0.0
        ):
            raise ValueError("threshold_radius must be finite and positive")
        object.__setattr__(self, "threshold_radius", float(self.threshold_radius))

    @property
    def stream_key(self) -> MonitorStreamKey:
        """Return the one exact monitor lineage allowed by this capture."""

        return MonitorStreamKey(
            stream_id=_STREAM_ID,
            metric=self.baseline.metric,
            threshold_stratum=self.baseline.threshold_stratum,
            configuration_identity=self.baseline.configuration_identity,
            data_identity=self.baseline.data_identity,
            flat_binding_id=self.baseline.flat_binding.identity_id,
            hnsw_binding_id=self.baseline.hnsw_binding.identity_id,
        )

    @property
    def route_binding(self) -> RouteStateBinding:
        """Return the exact LKG-only binding for runtime adapter checks."""

        return RouteStateBinding(
            metric=self.baseline.metric,
            threshold_stratum=self.baseline.threshold_stratum,
            last_known_good_ef=self.baseline.last_known_good_ef,
            configuration_identity=self.baseline.configuration_identity,
            data_identity=self.baseline.data_identity,
            flat_binding_id=self.baseline.flat_binding.identity_id,
            hnsw_binding_id=self.baseline.hnsw_binding.identity_id,
        )


@dataclass(frozen=True, slots=True)
class PreflightCaptureResult:
    """Small immutable result for callers that must not infer safety from I/O."""

    output_dir: Path
    complete: bool
    reason_codes: tuple[str, ...]
    call_counts: Mapping[str, int]


class ReadOnlyPreflightClient:
    """Record and strictly limit a live preflight to two read-only methods."""

    def __init__(self, client: ReadOnlyMilvusClientLike, *, allowed_collections: frozenset[str]) -> None:
        if not allowed_collections or not all(isinstance(value, str) and value for value in allowed_collections):
            raise ValueError("allowed_collections must contain non-empty names")
        if not callable(getattr(client, "get_load_state", None)) or not callable(
            getattr(client, "describe_index", None)
        ):
            raise TypeError("client must expose only required read-only methods")
        self._client = client
        self._allowed_collections = allowed_collections
        self._calls: list[dict[str, str]] = []

    @property
    def calls(self) -> tuple[dict[str, str], ...]:
        """Return copies of the complete ordered, non-sensitive call transcript."""

        return tuple(dict(call) for call in self._calls)

    @property
    def call_counts(self) -> dict[str, int]:
        """Return a stable count including zero-free permitted methods only."""

        counts = Counter(call["method"] for call in self._calls)
        return {name: int(counts[name]) for name in ("get_load_state", "describe_index")}

    def get_load_state(self, *, collection_name: str) -> object:
        """Forward one permitted collection-load read and record its intent."""

        self._validate_collection(collection_name)
        self._calls.append({"method": "get_load_state", "collection_name": collection_name})
        return self._client.get_load_state(collection_name=collection_name)

    def describe_index(self, *, collection_name: str, index_name: str) -> object:
        """Forward one permitted index-description read and record its intent."""

        self._validate_collection(collection_name)
        if index_name != INDEX_NAME:
            raise ValueError("preflight may describe only the configured vector index")
        self._calls.append(
            {
                "method": "describe_index",
                "collection_name": collection_name,
                "index_name": index_name,
            }
        )
        return self._client.describe_index(collection_name=collection_name, index_name=index_name)

    def _validate_collection(self, collection_name: object) -> None:
        if not isinstance(collection_name, str) or collection_name not in self._allowed_collections:
            raise ValueError("preflight collection is not baseline-bound")


def target_from_artifacts(*, baseline_path: Path, dataset_dir: Path) -> PreflightEvidenceTarget:
    """Load only verified DATASET-001 and reviewed baseline provenance."""

    baseline = load_identity_baseline(Path(baseline_path))
    bundle, thresholds, _manifest = load_dataset(Path(dataset_dir))
    try:
        index = THRESHOLD_LABELS.index(baseline.threshold_stratum)
        radius = thresholds[baseline.metric][index]
    except (KeyError, ValueError, IndexError) as error:
        raise ValueError("DATASET_THRESHOLD_PROVENANCE_INVALID") from error
    return PreflightEvidenceTarget(
        baseline=baseline,
        dataset_manifest_sha256=sha256_file(Path(dataset_dir) / "generation_manifest.json"),
        dimensions=int(bundle.base_vectors.shape[1]),
        threshold_radius=radius,
    )


def capture_read_only_preflight(
    *,
    target: PreflightEvidenceTarget,
    output_dir: Path,
    client: ReadOnlyMilvusClientLike,
    stack_health_probe: StackHealthProbeLike,
    repository: Path,
    utc_now: Callable[[], str],
) -> PreflightCaptureResult:
    """Capture one read-only adapter preflight and persist fail-closed evidence."""

    root = Path(output_dir)
    if root.exists() or root.is_symlink():
        raise ValueError("OUTPUT_PATH_EXISTS")
    if not isinstance(target, PreflightEvidenceTarget):
        raise TypeError("target must be a PreflightEvidenceTarget")
    timestamp = _valid_utc(utc_now)
    if timestamp is None:
        raise ValueError("PREFLIGHT_CLOCK_INVALID")
    if not isinstance(repository, Path) or not repository.is_dir():
        raise ValueError("REPOSITORY_INVALID")
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)

    facade = ReadOnlyPreflightClient(
        client,
        allowed_collections=frozenset(
            {
                target.baseline.flat_binding.expected.collection_name,
                target.baseline.hnsw_binding.expected.collection_name,
            }
        ),
    )
    pre_identity: dict[str, object] | None = None
    post_identity: dict[str, object] | None = None
    readiness: dict[str, object] | None = None
    slot_safety: dict[str, object] | None = None
    reasons: list[str] = []
    try:
        pre = _identity_snapshot(facade, target)
        pre_identity = _identity_document(pre)
        if not _matches_baseline(pre, target):
            reasons.append("PREFLIGHT_IDENTITY_BASELINE_MISMATCH")
        else:
            executor = MilvusRangeServingExecutor(
                client=facade,
                plans={
                    target.stream_key: HostServingPlan(
                        flat_collection_name=target.baseline.flat_binding.expected.collection_name,
                        hnsw_collection_name=target.baseline.hnsw_binding.expected.collection_name,
                        flat_binding=target.baseline.flat_binding,
                        hnsw_binding=target.baseline.hnsw_binding,
                        threshold_radius=target.threshold_radius,
                        dimensions=target.dimensions,
                        allowed_served_efs=frozenset({target.baseline.last_known_good_ef}),
                    )
                },
                stack_health_probe=stack_health_probe,
            )
            probe = Stage4ServingRuntimeProbe(
                expected_binding=target.route_binding,
                expected_stream=target.stream_key,
                serving_preflight=executor,
                utc_now=lambda: timestamp,
            )
            observed_readiness = probe.preflight(binding=target.route_binding)
            readiness = _readiness_document(observed_readiness)
            if not observed_readiness.serving_preflight_complete:
                reasons.extend(("PREFLIGHT_RUNTIME_INCOMPLETE", *observed_readiness.reason_codes))
            else:
                observed_safety = probe.slot_safety(binding=target.route_binding)
                slot_safety = _slot_safety_document(observed_safety)
                if not (observed_safety.health_ok and observed_safety.identity_ok):
                    reasons.append("PREFLIGHT_SLOT_SAFETY_UNSAFE")
                    if observed_safety.reason_code is not None:
                        reasons.append(observed_safety.reason_code)
                else:
                    post = _identity_snapshot(facade, target)
                    post_identity = _identity_document(post)
                    if not _matches_baseline(post, target):
                        reasons.append("PREFLIGHT_IDENTITY_BASELINE_MISMATCH")
                    elif pre != post:
                        reasons.append("PREFLIGHT_IDENTITY_CHANGED")
    except Exception:
        reasons.append("PREFLIGHT_EXECUTION_UNAVAILABLE")

    call_counts = facade.call_counts
    complete = not reasons and call_counts == _SUCCESS_COUNTS
    if not reasons and not complete:
        reasons.append("PREFLIGHT_CALL_TRANSCRIPT_INVALID")
    result = _result_document(
        target=target,
        timestamp=timestamp,
        repository=repository,
        pre_identity=pre_identity,
        post_identity=post_identity,
        readiness=readiness,
        slot_safety=slot_safety,
        calls=facade.calls,
        call_counts=call_counts,
        complete=not reasons,
        reason_codes=tuple(dict.fromkeys(reasons)),
    )
    _write_bundle(root, result)
    return PreflightCaptureResult(
        output_dir=root,
        complete=result["status"] == "COMPLETE",
        reason_codes=tuple(result["reason_codes"]),  # type: ignore[arg-type]
        call_counts=MappingProxyType(dict(call_counts)),
    )


def verify_preflight_evidence(
    output_dir: Path,
    *,
    target: PreflightEvidenceTarget,
    require_complete: bool = True,
) -> dict[str, object]:
    """Independently validate one immutable read-only preflight bundle."""

    if not isinstance(target, PreflightEvidenceTarget):
        raise TypeError("target must be a PreflightEvidenceTarget")
    root = Path(output_dir)
    expected = {"preflight_result.json", "manifest.json", "execution_receipt.json"}
    if root.is_symlink() or not root.is_dir() or {path.name for path in root.iterdir()} != expected:
        raise ValueError("PREFLIGHT_EVIDENCE_STRUCTURE_INVALID")
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ValueError("PREFLIGHT_EVIDENCE_SYMLINK_REFUSED")
    result = _load_json(root / "preflight_result.json")
    manifest = _load_json(root / "manifest.json")
    receipt = _load_json(root / "execution_receipt.json")
    _verify_self_hash(result)
    _verify_self_hash(manifest)
    _verify_self_hash(receipt)
    if (
        frozenset(result) != _RESULT_FIELDS
        or frozenset(manifest) != _MANIFEST_FIELDS
        or frozenset(receipt) != _RECEIPT_FIELDS
        or manifest.get("schema_version") != _MANIFEST_SCHEMA_VERSION
        or receipt.get("schema_version") != _RECEIPT_SCHEMA_VERSION
        or result.get("schema_version") != _SCHEMA_VERSION
        or manifest.get("result_file_sha256") != sha256_file(root / "preflight_result.json")
        or receipt.get("result_self_sha256") != result.get("self_sha256")
        or receipt.get("manifest_self_sha256") != manifest.get("self_sha256")
        or receipt.get("status") != result.get("status")
        or manifest.get("status") != result.get("status")
        or result.get("target") != _target_document(target)
    ):
        raise ValueError("PREFLIGHT_EVIDENCE_LINKAGE_INVALID")
    status = result.get("status")
    counts = result.get("call_counts")
    if (
        status not in {"COMPLETE", "INCOMPLETE"}
        or not _valid_result_document(result)
        or not _valid_counts(counts)
    ):
        raise ValueError("PREFLIGHT_EVIDENCE_RESULT_INVALID")
    if status == "COMPLETE":
        if (
            result.get("reason_codes") != []
            or counts != _SUCCESS_COUNTS
            or result.get("pre_identity") != result.get("post_identity")
            or result.get("pre_identity") != _expected_identity_document(target)
            or not _complete_runtime_document(result.get("readiness"), result.get("slot_safety"))
            or result.get("no_effect_assertion") != _EFFECT_ASSERTION
        ):
            raise ValueError("PREFLIGHT_EVIDENCE_COMPLETE_INVALID")
    if require_complete and status != "COMPLETE":
        raise ValueError("PREFLIGHT_EVIDENCE_INCOMPLETE")
    return {"status": status, "call_counts": dict(counts), "result_sha256": result["self_sha256"]}


def _identity_snapshot(
    client: ReadOnlyPreflightClient, target: PreflightEvidenceTarget
) -> dict[IndexTrack, CollectionIdentity]:
    harness = MilvusHarness(client, dimensions=target.dimensions)
    return {
        IndexTrack.FLAT: harness.index_identity(
            target.baseline.flat_binding.expected.collection_name, Metric.L2, IndexTrack.FLAT
        ),
        IndexTrack.HNSW: harness.index_identity(
            target.baseline.hnsw_binding.expected.collection_name, Metric.L2, IndexTrack.HNSW
        ),
    }


def _matches_baseline(
    identities: Mapping[IndexTrack, CollectionIdentity], target: PreflightEvidenceTarget
) -> bool:
    try:
        return bool(
            target.baseline.flat_binding.matches(identities[IndexTrack.FLAT])
            and target.baseline.hnsw_binding.matches(identities[IndexTrack.HNSW])
        )
    except Exception:
        return False


def _identity_document(identities: Mapping[IndexTrack, CollectionIdentity]) -> dict[str, object]:
    return {
        track.value: {
            "collection_name": identity.collection_name,
            "metric": identity.metric,
            "index_track": identity.index_track,
            "description": _json_value(identity.description),
        }
        for track, identity in identities.items()
    }


def _readiness_document(value: object) -> dict[str, object]:
    return {
        "serving_preflight_complete": bool(getattr(value, "serving_preflight_complete", False)),
        "observed_at_utc": str(getattr(value, "observed_at_utc", "")),
        "reason_codes": list(getattr(value, "reason_codes", ())),
    }


def _slot_safety_document(value: object) -> dict[str, object]:
    return {
        "health_ok": bool(getattr(value, "health_ok", False)),
        "identity_ok": bool(getattr(value, "identity_ok", False)),
        "reason_code": getattr(value, "reason_code", None),
    }


def _result_document(
    *,
    target: PreflightEvidenceTarget,
    timestamp: str,
    repository: Path,
    pre_identity: dict[str, object] | None,
    post_identity: dict[str, object] | None,
    readiness: dict[str, object] | None,
    slot_safety: dict[str, object] | None,
    calls: tuple[dict[str, str], ...],
    call_counts: dict[str, int],
    complete: bool,
    reason_codes: tuple[str, ...],
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "status": "COMPLETE" if complete else "INCOMPLETE",
        "captured_at_utc": timestamp,
        "git": git_state(repository),
        "target": _target_document(target),
        "pre_identity": pre_identity,
        "post_identity": post_identity,
        "readiness": readiness,
        "slot_safety": slot_safety,
        "client_calls": [dict(call) for call in calls],
        "call_counts": call_counts,
        "reason_codes": list(reason_codes),
        "no_effect_assertion": {
            **_EFFECT_ASSERTION,
        },
    }
    document["self_sha256"] = _digest(document)
    return document


def _write_bundle(root: Path, result: Mapping[str, object]) -> None:
    result_path = root / "preflight_result.json"
    _write_immutable_json(result_path, dict(result))
    manifest: dict[str, object] = {
        "schema_version": _MANIFEST_SCHEMA_VERSION,
        "status": result["status"],
        "result_file_sha256": sha256_file(result_path),
        "result_self_sha256": result["self_sha256"],
    }
    manifest["self_sha256"] = _digest(manifest)
    _write_immutable_json(root / "manifest.json", manifest)
    receipt: dict[str, object] = {
        "schema_version": _RECEIPT_SCHEMA_VERSION,
        "status": result["status"],
        "result_self_sha256": result["self_sha256"],
        "manifest_self_sha256": manifest["self_sha256"],
    }
    receipt["self_sha256"] = _digest(receipt)
    _write_immutable_json(root / "execution_receipt.json", receipt)


def _write_immutable_json(path: Path, value: Mapping[str, object]) -> None:
    """Durably publish one canonical JSON document without replacement races."""

    if path.exists() or path.is_symlink():
        raise ValueError("PREFLIGHT_EVIDENCE_OUTPUT_EXISTS")
    payload = canonical_json_bytes(dict(value))
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _target_document(target: PreflightEvidenceTarget) -> dict[str, object]:
    return {
        "baseline_sha256": target.baseline.sha256,
        "dataset_manifest_sha256": target.dataset_manifest_sha256,
        "stream_key": _json_value(asdict(target.stream_key)),
        "route_binding": _json_value(asdict(target.route_binding)),
        "dimensions": target.dimensions,
        "threshold_radius": target.threshold_radius,
    }


def _expected_identity_document(target: PreflightEvidenceTarget) -> dict[str, object]:
    return _identity_document(
        {
            IndexTrack.FLAT: target.baseline.flat_binding.expected,
            IndexTrack.HNSW: target.baseline.hnsw_binding.expected,
        }
    )


def _valid_utc(clock: Callable[[], str]) -> str | None:
    try:
        value = clock()
    except Exception:
        return None
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        return None
    return value


def _json_value(value: object) -> object:
    """Accept only finite, JSON-native values from trusted index metadata."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("PREFLIGHT_NONFINITE_JSON_VALUE")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("PREFLIGHT_NON_STRING_JSON_KEY")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise ValueError("PREFLIGHT_UNSUPPORTED_JSON_VALUE")


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("PREFLIGHT_EVIDENCE_JSON_INVALID") from error
    if not isinstance(value, dict):
        raise ValueError("PREFLIGHT_EVIDENCE_DOCUMENT_INVALID")
    return value


def _verify_self_hash(value: dict[str, object]) -> None:
    projection = dict(value)
    expected = projection.pop("self_sha256", None)
    if not isinstance(expected, str) or _SHA256.fullmatch(expected) is None or _digest(projection) != expected:
        raise ValueError("PREFLIGHT_EVIDENCE_SELF_HASH_INVALID")


def _valid_counts(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"get_load_state", "describe_index"}
        and all(type(count) is int and count >= 0 for count in value.values())
    )


def _valid_result_document(result: Mapping[str, object]) -> bool:
    """Reject noncanonical or self-consistently rehashed evidence documents."""

    if (
        result.get("schema_version") != _SCHEMA_VERSION
        or _valid_utc(lambda: result.get("captured_at_utc")) is None
        or result.get("no_effect_assertion") != _EFFECT_ASSERTION
        or not _valid_target_document(result.get("target"))
        or not _valid_git_document(result.get("git"))
        or not _valid_transcript(result.get("client_calls"), result.get("call_counts"))
    ):
        return False
    reasons = result.get("reason_codes")
    if not isinstance(reasons, list) or not all(
        isinstance(reason, str) and _CODE.fullmatch(reason) is not None for reason in reasons
    ):
        return False
    return (result.get("status") == "COMPLETE") is (not reasons)


def _valid_target_document(value: object) -> bool:
    if not isinstance(value, Mapping) or frozenset(value) != {
        "baseline_sha256", "dataset_manifest_sha256", "stream_key", "route_binding",
        "dimensions", "threshold_radius",
    }:
        return False
    if not all(
        isinstance(value[name], str) and _SHA256.fullmatch(value[name]) is not None
        for name in ("baseline_sha256", "dataset_manifest_sha256")
    ):
        return False
    if (
        type(value["dimensions"]) is not int
        or value["dimensions"] <= 0
        or not isinstance(value["threshold_radius"], (int, float))
        or isinstance(value["threshold_radius"], bool)
        or not math.isfinite(float(value["threshold_radius"]))
        or float(value["threshold_radius"]) <= 0.0
    ):
        return False
    stream, binding = value["stream_key"], value["route_binding"]
    if not isinstance(stream, Mapping) or not isinstance(binding, Mapping):
        return False
    stream_fields = {
        "stream_id", "metric", "threshold_stratum", "configuration_identity",
        "data_identity", "flat_binding_id", "hnsw_binding_id",
    }
    binding_fields = stream_fields - {"stream_id"} | {"last_known_good_ef"}
    if frozenset(stream) != stream_fields or frozenset(binding) != binding_fields:
        return False
    return bool(
        stream.get("stream_id") == _STREAM_ID
        and stream.get("metric") == binding.get("metric") == Metric.L2.value
        and stream.get("threshold_stratum") == binding.get("threshold_stratum") == "target-075"
        and binding.get("last_known_good_ef") == 400
        and all(
            isinstance(stream.get(name), str) and bool(stream[name])
            for name in stream_fields - {"metric"}
        )
        and all(
            stream.get(name) == binding.get(name)
            for name in (
                "metric", "threshold_stratum", "configuration_identity", "data_identity",
                "flat_binding_id", "hnsw_binding_id",
            )
        )
    )


def _valid_git_document(value: object) -> bool:
    return bool(
        isinstance(value, Mapping)
        and frozenset(value) == {"commit", "dirty"}
        and isinstance(value.get("commit"), str)
        and re.fullmatch(r"[0-9a-f]{40}", value["commit"]) is not None
        and type(value.get("dirty")) is bool
    )


def _valid_transcript(calls: object, counts: object) -> bool:
    if not isinstance(calls, list) or not _valid_counts(counts):
        return False
    normalized: list[str] = []
    for call in calls:
        if not isinstance(call, Mapping) or not isinstance(call.get("method"), str):
            return False
        method = call["method"]
        if method == "get_load_state":
            if frozenset(call) != {"method", "collection_name"}:
                return False
        elif method == "describe_index":
            if frozenset(call) != {"method", "collection_name", "index_name"} or call.get("index_name") != INDEX_NAME:
                return False
        else:
            return False
        if not isinstance(call.get("collection_name"), str) or not call["collection_name"]:
            return False
        normalized.append(method)
    return Counter(normalized) == Counter(counts)


def _complete_runtime_document(readiness: object, safety: object) -> bool:
    return bool(
        isinstance(readiness, dict)
        and readiness == {
            "serving_preflight_complete": True,
            "observed_at_utc": readiness.get("observed_at_utc"),
            "reason_codes": [],
        }
        and isinstance(readiness.get("observed_at_utc"), str)
        and _valid_utc(lambda: readiness["observed_at_utc"]) is not None
        and safety == {"health_ok": True, "identity_ok": True, "reason_code": None}
    )
