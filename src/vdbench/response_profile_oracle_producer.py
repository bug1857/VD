"""Independent exact-oracle producer for one frozen response-profile population.

Purpose:
    Compute exact ground truth for the 1,200 frozen calibration members of one
    already-governed ``CalibrationPopulationManifest``, independently of the
    system under test, and freeze it into the existing
    ``ResponseProfileOracleManifest`` contract so it can later feed
    ``prepare_exp011_acquisition_inputs``.
Independence:
    The oracle is computed only from (a) the population's own governed query
    material and (b) an independently verified DATASET-001 base-vector corpus.
    It never reads an HNSW acquisition result, an EXP-011 measured response, a
    policy decision, a detector result beyond the population identity already
    frozen into the members, a route, a grant, or any live canary state. This
    module imports no Milvus, acquisition, policy, actuation, canary, grant, or
    routing code and is fully computable with Milvus absent.
Base-vector authority:
    The corpus is proven, not asserted. ``verify_dataset_artifacts`` re-checks
    every DATASET-001 artifact digest (including ``base_vectors.npy``) against
    ``generation_manifest.json`` and ``SHA256SUMS``; the governed base data
    identity is then re-derived from the verified manifest using the accepted
    ``<dataset version>:sha256:<generation manifest sha256>`` convention already
    used by EXP-005 and the Stage-4 workload binding. Every member's source
    namespace must bind that exact corpus, or production fails closed.
Exactness:
    Scores, thresholds, ordering, capping, and ``full_count`` come from the
    unchanged accepted oracle (``vdbench.oracle.exact_range_search``). This
    module introduces no metric or range semantics of its own and applies no
    tolerance: no ``isclose``, no ``allclose``, and no ULP forgiveness.
Outputs:
    Ordered ``ResponseProfileOracleRecord`` values, the built
    ``ResponseProfileOracleManifest``, the proven corpus identities, and one
    domain-separated producer evidence digest. No profile, freshness, policy,
    grant, routing, or candidate authority is created.
Failure modes:
    An invalid/incomplete population, an unverifiable or mismatched corpus, a
    dimension/metric/stratum mismatch, or a member whose namespace does not bind
    the proven corpus fails closed before any oracle value is computed.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import numpy.typing as npt

from .artifacts import canonical_json_bytes, sha256_file, verify_dataset_artifacts
from .config import ContractViolation, Metric
from .oracle import exact_range_search
from .response_profile_evidence import (
    CALIBRATION_QUERY_COUNT,
    ArtifactSourceNamespace,
    CalibrationPopulationManifest,
    LiveStreamSourceNamespace,
    ResponseProfileEvidenceContractError,
    verify_calibration_population_manifest,
)
from .response_profile_semantic import (
    ResponseProfileOracleManifest,
    ResponseProfileOracleRecord,
    ResponseProfileSemanticError,
    build_response_profile_oracle_manifest,
    build_response_profile_oracle_record,
)


__all__ = [
    "ORACLE_PRODUCER_SCHEMA_VERSION",
    "ResponseProfileOracleProducerError",
    "ResponseProfileOracleProduct",
    "produce_response_profile_oracle",
]


ORACLE_PRODUCER_SCHEMA_VERSION = "response-profile-oracle-producer-v1"
_PRODUCER_DOMAIN = b"VD::RESPONSE_PROFILE_ORACLE_PRODUCER::V1\x00"
_EXPECTED_DATASET_ID = "DATASET-001"


class ResponseProfileOracleProducerError(RuntimeError):
    """Fail-closed producer error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> ResponseProfileOracleProducerError:
    return ResponseProfileOracleProducerError(code, message)


@dataclass(frozen=True, slots=True)
class ResponseProfileOracleProduct:
    """Independently computed oracle evidence for one frozen population.

    This value is predictive ground truth plus its proven corpus identities. It
    is not a profile, not freshness evidence, and not candidate authority.
    """

    schema_version: str
    base_data_identity: str
    generation_manifest_sha256: str
    base_vectors_sha256: str
    base_ids_sha256: str
    workload_manifest_sha256: str
    ordered_query_payload_sha256: str
    metric: Metric
    threshold_stratum: str
    dimensions: int
    records: tuple[ResponseProfileOracleRecord, ...]
    oracle_manifest: ResponseProfileOracleManifest
    oracle_producer_sha256: str


def _verified_population(
    population: CalibrationPopulationManifest,
) -> CalibrationPopulationManifest:
    if type(population) is not CalibrationPopulationManifest:
        raise _error("ORACLE_POPULATION_INVALID", "population must be concrete")
    try:
        verified = verify_calibration_population_manifest(population)
    except (AttributeError, TypeError, ValueError) as exc:
        raise _error("ORACLE_POPULATION_INVALID", "population failed verification") from exc
    members = verified.calibration_role_manifest.members
    if len(members) != CALIBRATION_QUERY_COUNT:
        raise _error(
            "ORACLE_POPULATION_COUNT_INVALID",
            f"oracle production requires exactly {CALIBRATION_QUERY_COUNT} members",
        )
    return verified


def _load_verified_corpus(
    dataset001_dir: Path,
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.float32], dict[str, str], Mapping[str, Any]]:
    """Verify and load the DATASET-001 corpus that will answer every query.

    ``verify_dataset_artifacts`` is the accepted, unchanged verifier: it
    re-hashes every artifact against the generation manifest and SHA256SUMS, so
    a corpus that does not match its own committed identity cannot be used.
    """

    directory = Path(dataset001_dir)
    try:
        manifest = verify_dataset_artifacts(directory)
    except (OSError, ValueError, KeyError, ContractViolation) as exc:
        raise _error("ORACLE_CORPUS_UNVERIFIED", "DATASET-001 artifacts failed verification") from exc
    dataset = manifest.get("dataset") if isinstance(manifest, Mapping) else None
    if not isinstance(dataset, Mapping) or dataset.get("dataset_id") != _EXPECTED_DATASET_ID:
        raise _error("ORACLE_CORPUS_INVALID", "corpus is not DATASET-001")
    version = dataset.get("version")
    if not isinstance(version, str) or not version:
        raise _error("ORACLE_CORPUS_INVALID", "corpus version is invalid")
    try:
        base_ids = np.load(directory / "base_ids.npy", allow_pickle=False)
        base_vectors = np.load(directory / "base_vectors.npy", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise _error("ORACLE_CORPUS_INVALID", "corpus arrays could not be loaded") from exc
    if (
        base_ids.ndim != 1
        or base_vectors.ndim != 2
        or base_ids.shape[0] != base_vectors.shape[0]
        or base_ids.shape[0] == 0
        or base_ids.dtype.kind not in "iu"
        or base_vectors.dtype.str != "<f4"
        or not bool(np.all(np.isfinite(base_vectors)))
        or len(np.unique(base_ids)) != base_ids.size
    ):
        raise _error("ORACLE_CORPUS_INVALID", "corpus base arrays are invalid")
    identities = {
        "version": version,
        "generation_manifest_sha256": sha256_file(directory / "generation_manifest.json"),
        "base_ids_sha256": sha256_file(directory / "base_ids.npy"),
        "base_vectors_sha256": sha256_file(directory / "base_vectors.npy"),
    }
    return (
        np.asarray(base_ids, dtype=np.int64),
        np.asarray(base_vectors, dtype="<f4"),
        identities,
        dataset,
    )


def _namespace_binds_corpus(
    namespace: object, *, base_data_identity: str, generation_manifest_sha256: str
) -> bool:
    """Both governed namespace forms must bind the exact proven corpus."""

    if type(namespace) is ArtifactSourceNamespace:
        return namespace.generation_manifest_sha256 == generation_manifest_sha256
    if type(namespace) is LiveStreamSourceNamespace:
        return namespace.data_identity == base_data_identity
    return False


def produce_response_profile_oracle(
    *,
    population: CalibrationPopulationManifest,
    dataset001_dir: Path,
    expected_base_data_identity: str,
    expected_metric: Metric,
    expected_threshold_stratum: str,
    expected_dimensions: int,
) -> ResponseProfileOracleProduct:
    """Independently compute the exact oracle for one frozen population.

    Every input is a governed value or an independently verifiable corpus; no
    acquisition result, measured response, or policy value may be supplied. The
    1,200 records are produced in canonical population order and are never
    reordered by caller input.
    """

    verified = _verified_population(population)
    members = verified.calibration_role_manifest.members

    if type(expected_metric) is not Metric:
        raise _error("ORACLE_METRIC_INVALID", "expected metric must be concrete")
    if not isinstance(expected_threshold_stratum, str) or not expected_threshold_stratum:
        raise _error("ORACLE_STRATUM_INVALID", "expected threshold stratum must be text")
    if isinstance(expected_dimensions, bool) or not isinstance(expected_dimensions, int) or expected_dimensions <= 0:
        raise _error("ORACLE_DIMENSIONS_INVALID", "expected dimensions must be a positive integer")
    if not isinstance(expected_base_data_identity, str) or not expected_base_data_identity:
        raise _error("ORACLE_DATA_IDENTITY_INVALID", "expected base data identity must be text")

    cell = verified.cell
    if cell.metric is not expected_metric or cell.threshold_stratum != expected_threshold_stratum:
        raise _error(
            "ORACLE_CELL_MISMATCH",
            "population cell does not match the expected metric/threshold stratum",
        )

    base_ids, base_vectors, identities, _dataset = _load_verified_corpus(dataset001_dir)
    generation_manifest_sha256 = identities["generation_manifest_sha256"]
    # Accepted convention, re-derived from the verified manifest rather than
    # asserted by the caller (EXP-005 / Stage-4 workload binding use the same
    # "<version>:sha256:<generation manifest sha256>" form).
    base_data_identity = f"{identities['version']}:sha256:{generation_manifest_sha256}"
    if base_data_identity != expected_base_data_identity:
        raise _error(
            "ORACLE_DATA_IDENTITY_MISMATCH",
            "verified corpus does not match the expected base data identity",
        )
    if int(base_vectors.shape[1]) != expected_dimensions:
        raise _error("ORACLE_DIMENSIONS_MISMATCH", "corpus dimensions differ from expected")

    records: list[ResponseProfileOracleRecord] = []
    for member in members:
        if not _namespace_binds_corpus(
            member.source_namespace,
            base_data_identity=base_data_identity,
            generation_manifest_sha256=generation_manifest_sha256,
        ):
            raise _error(
                "ORACLE_MEMBER_CORPUS_MISMATCH",
                "a member's source namespace does not bind the proven corpus",
            )
        payload = member.query_payload_identity
        if payload.metric is not expected_metric or payload.threshold_stratum != expected_threshold_stratum:
            raise _error("ORACLE_CELL_MISMATCH", "member payload does not match the expected cell")
        vector_identity = member.vector_identity
        if vector_identity.dimensions != expected_dimensions:
            raise _error("ORACLE_DIMENSIONS_MISMATCH", "member vector dimensions differ from expected")
        query = np.frombuffer(vector_identity.canonical_vector_bytes, dtype="<f4")
        if query.shape != (expected_dimensions,):
            raise _error("ORACLE_DIMENSIONS_MISMATCH", "member query vector shape differs from expected")
        try:
            result = exact_range_search(
                base_vectors,
                base_ids,
                query,
                expected_metric,
                radius=payload.radius,
                range_filter=payload.range_filter,
                limit=payload.limit,
            )
        except ContractViolation as exc:
            raise _error("ORACLE_COMPUTATION_INVALID", "exact oracle refused a member query") from exc
        try:
            records.append(
                build_response_profile_oracle_record(
                    observation_identity_sha256=member.observation_identity.observation_identity_sha256,
                    query_id_sha256=member.query_identity.query_id_sha256,
                    query_payload_sha256=payload.query_payload_sha256,
                    limit=payload.limit,
                    full_count=result.full_count,
                    capped_ids=tuple(hit.id for hit in result.hits),
                    capped_distances=tuple(hit.score for hit in result.hits),
                    metric=expected_metric,
                    radius=payload.radius,
                    range_filter=payload.range_filter,
                )
            )
        except (ResponseProfileSemanticError, TypeError, ValueError) as exc:
            raise _error("ORACLE_RECORD_INVALID", "exact oracle record failed its contract") from exc

    try:
        oracle_manifest = build_response_profile_oracle_manifest(
            population=verified, records=tuple(records)
        )
    except (ResponseProfileEvidenceContractError, ResponseProfileSemanticError, TypeError, ValueError) as exc:
        raise _error("ORACLE_MANIFEST_INVALID", "oracle manifest failed its contract") from exc

    producer_payload = {
        "schema_version": ORACLE_PRODUCER_SCHEMA_VERSION,
        "base_data_identity": base_data_identity,
        "generation_manifest_sha256": generation_manifest_sha256,
        "base_ids_sha256": identities["base_ids_sha256"],
        "base_vectors_sha256": identities["base_vectors_sha256"],
        "workload_manifest_sha256": verified.workload_manifest_sha256,
        "ordered_query_payload_sha256": verified.ordered_query_payload_sha256,
        "oracle_manifest_sha256": oracle_manifest.oracle_manifest_sha256,
        "metric": expected_metric.value,
        "threshold_stratum": expected_threshold_stratum,
        "dimensions": expected_dimensions,
        "record_count": len(records),
    }
    producer_digest = hashlib.sha256(
        _PRODUCER_DOMAIN + canonical_json_bytes(producer_payload)
    ).hexdigest()

    return ResponseProfileOracleProduct(
        schema_version=ORACLE_PRODUCER_SCHEMA_VERSION,
        base_data_identity=base_data_identity,
        generation_manifest_sha256=generation_manifest_sha256,
        base_vectors_sha256=identities["base_vectors_sha256"],
        base_ids_sha256=identities["base_ids_sha256"],
        workload_manifest_sha256=verified.workload_manifest_sha256,
        ordered_query_payload_sha256=verified.ordered_query_payload_sha256,
        metric=expected_metric,
        threshold_stratum=expected_threshold_stratum,
        dimensions=expected_dimensions,
        records=tuple(records),
        oracle_manifest=oracle_manifest,
        oracle_producer_sha256=producer_digest,
    )
