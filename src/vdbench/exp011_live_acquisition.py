"""EXP-011 real acquisition composition root -- NOT invoked by this module's
own import, tests, or any code in this repository.

Purpose:
    Wire the real Milvus adapters (`response_profile_milvus_adapter.py`) into
    the unmodified `ResponseProfileProducer`/R2-D/R2-E machinery to produce
    real PROSPECTIVE evidence, once a separately authorized operator actually
    runs it against a governed read-only Milvus stack. This module never runs
    itself; `main`/`run_exp011_live_acquisition` execute only when explicitly
    invoked by an operator with a real client.
Inputs:
    A real (or, in tests, fake) Milvus client plus every already-frozen
    governed value this run must reproduce exactly: `ResponseProfileRunBinding`,
    `ResponseProfileStaticIdentity`, `ResponseProfileControl`, and
    `ResponseProfileOracleManifest`. This module does not construct or freeze
    any of those itself -- it only consumes already-governed values, exactly
    as `response_profile_producer.py` requires.
Outputs:
    One immutable JSON manifest per run, in a caller-chosen output directory,
    labeled with the caller-supplied `evidence_status` -- never hardcoded to
    "PROSPECTIVE" by this module itself; only a real operator invocation
    (via `main`, never exercised here) uses that value.
Dependencies:
    `response_profile_producer.py` (unmodified), `response_profile_root_pin.py`
    (unmodified R2-D), `response_profile_projection.py` (unmodified R2-E),
    `response_profile_lifecycle_ledger.py` (unmodified), and
    `response_profile_milvus_adapter.py`. No policy, admission, grant,
    activation, or route-authority module is imported.
Failure modes:
    A producer result that is not `complete` is recorded in the manifest with
    its exact reason codes and no profile/capability fields; this module
    never fabricates a profile or capability from an incomplete run.
Governed artifact loading (`main`'s `--*-json` / `--vector-material` args):
    All four governed artifacts are loaded for real, strict and fail-closed,
    and cross-validated BEFORE any Milvus client, ledger, or lifecycle STARTED
    is constructed:

    * `--control-json` -> `response_profile_control_from_document`
    * `--static-identity-json` -> `response_profile_static_identity_from_document`
    * `--vector-material` -> `load_response_profile_vector_material`
      (the supplemental `response-profile-vector-material-v1` artifact)
    * `--run-binding-json` + verified vector material ->
      `response_profile_run_binding_from_document`
    * `--oracle-manifest-json` + verified vector material ->
      `response_profile_oracle_manifest_from_document`

    `ResponseProfileRunBinding`/`ResponseProfileOracleManifest` cannot be
    reconstructed from their own canonical documents alone: those documents are
    digest-only, and `response_profile_evidence.py`'s `role_manifest_document`
    -- the one canonical document that carries full member payloads -- records
    only each member's `vector_sha256`, never the raw
    `QueryVectorIdentity.canonical_vector_bytes`, by the module's own stated
    design ("Digests ... are not signatures and do not authenticate an external
    raw artifact"). The supplemental `--vector-material` artifact
    (`response_profile_vector_material.py`) closes exactly that gap: it
    transports the two existing `role_manifest_document` values verbatim (their
    meaning is unchanged) alongside a separate raw-canonical-vector-bytes
    section, binding each vector by role + position + `vector_sha256` +
    dimensions. It is fully verified but NON-AUTHORIZING: each loader
    reconstructs the governed object through the existing contract factories and
    then requires the reconstruction's own canonical document to be
    byte-identical to the authoritative input document, so a vector material for
    a different run (or a tampered one) is rejected and can never override the
    governed digests.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from .artifacts import write_immutable_json
from .config import Metric
from .response_profile import CalibratedResponseProfile
from .response_profile_control import (
    ResponseProfileControl,
    ResponseProfileControlError,
    response_profile_control_from_document,
    verify_response_profile_control,
)
from .response_profile_lifecycle import ResponseProfileRunBinding
from .response_profile_lifecycle_ledger import ResponseProfileLifecycleLedger
from .response_profile_milvus_adapter import (
    ResponseProfileMilvusClientLike,
    ResponseProfileMilvusQueryExecutor,
    ResponseProfileMilvusRuntimeProbe,
    StackHealthProbeLike,
    build_response_profile_milvus_client,
)
from .response_profile_producer import (
    ResponseProfileClock,
    ResponseProfileProducer,
    ResponseProfileProducerResult,
)
from .response_profile_projection import project_root_pinned_response_profile
from .response_profile_root_pin import (
    RootPinnedResponseProfileEvidence,
    issue_root_pinned_response_profile_evidence,
)
from .response_profile_semantic import (
    ResponseProfileOracleManifest,
    ResponseProfileSemanticBundle,
    ResponseProfileSemanticError,
    ResponseProfileSemanticExpectation,
    ResponseProfileStaticIdentity,
    build_response_profile_identity_from_static,
    response_profile_static_identity_from_document,
)
from .response_profile_vector_material import (
    ResponseProfileVectorMaterialError,
    VerifiedResponseProfileVectorMaterial,
    load_response_profile_vector_material,
    response_profile_oracle_manifest_from_document,
    response_profile_run_binding_from_document,
)


__all__ = [
    "Exp011LiveAcquisitionError",
    "Exp011LiveAcquisitionResult",
    "run_exp011_live_acquisition",
    "run_exp011_live_acquisition_from_cli",
    "load_control_artifact",
    "load_static_identity_artifact",
    "load_vector_material_artifact",
    "load_run_binding_artifact",
    "load_oracle_manifest_artifact",
    "main",
]


class Exp011LiveAcquisitionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Exp011LiveAcquisitionResult:
    output_dir: Path
    manifest_path: Path
    evidence_status: str
    producer_complete: bool
    producer_reason_codes: tuple[str, ...]
    profile: CalibratedResponseProfile | None
    root_pinned_capability: RootPinnedResponseProfileEvidence | None


def run_exp011_live_acquisition(
    *,
    client: ResponseProfileMilvusClientLike,
    stack_health_probe: StackHealthProbeLike,
    collection_name: str,
    dimensions: int,
    metric: Metric,
    ledger_path: Path,
    run_binding: ResponseProfileRunBinding,
    static_identity: ResponseProfileStaticIdentity,
    control: ResponseProfileControl,
    oracle_manifest: ResponseProfileOracleManifest,
    output_dir: Path,
    evidence_status: str,
    max_blocks: int | None = None,
    clock: ResponseProfileClock | None = None,
) -> Exp011LiveAcquisitionResult:
    """Drive the unmodified `ResponseProfileProducer` against a real (or, in
    tests, fake) Milvus client and durably record the outcome.

    `evidence_status` is a required, caller-supplied label -- this function
    never assumes "PROSPECTIVE" on the caller's behalf. Only `main` (a real
    operator invocation) passes "PROSPECTIVE"; every offline/test call in
    this repository passes "STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE".
    """

    if not isinstance(evidence_status, str) or not evidence_status:
        raise Exp011LiveAcquisitionError("evidence_status must be a non-empty string")
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise Exp011LiveAcquisitionError(f"refusing to overwrite existing directory: {output_dir}")
    output_dir.mkdir(parents=True, mode=0o700)

    executor = ResponseProfileMilvusQueryExecutor(
        client, collection_name=collection_name, dimensions=dimensions
    )
    probe = ResponseProfileMilvusRuntimeProbe(
        client,
        collection_name=collection_name,
        dimensions=dimensions,
        metric=metric,
        stack_health_probe=stack_health_probe,
    )
    ledger = ResponseProfileLifecycleLedger(ledger_path, expected_run_binding=run_binding)
    try:
        producer = ResponseProfileProducer(
            ledger=ledger,
            run_binding=run_binding,
            static_identity=static_identity,
            control=control,
            oracle_manifest=oracle_manifest,
            query_executor=executor,
            runtime_probe=probe,
            clock=clock,
        )
        result = producer.run(max_blocks=max_blocks)

        profile: CalibratedResponseProfile | None = None
        capability: RootPinnedResponseProfileEvidence | None = None
        if result.complete:
            profile, capability = _root_pin_and_project(
                ledger=ledger,
                run_binding=run_binding,
                control=control,
                oracle_manifest=oracle_manifest,
                result=result,
            )
    finally:
        ledger.close()

    manifest_path = output_dir / "manifest.json"
    write_immutable_json(manifest_path, _manifest_document(
        evidence_status=evidence_status,
        collection_name=collection_name,
        run_binding_sha256=run_binding.run_binding_sha256,
        control_sha256=control.control_profile_sha256,
        result=result,
        profile=profile,
        capability=capability,
    ))

    return Exp011LiveAcquisitionResult(
        output_dir=output_dir,
        manifest_path=manifest_path,
        evidence_status=evidence_status,
        producer_complete=result.complete,
        producer_reason_codes=result.reason_codes,
        profile=profile,
        root_pinned_capability=capability,
    )


def _root_pin_and_project(
    *,
    ledger: ResponseProfileLifecycleLedger,
    run_binding: ResponseProfileRunBinding,
    control: ResponseProfileControl,
    oracle_manifest: ResponseProfileOracleManifest,
    result: ResponseProfileProducerResult,
) -> tuple[CalibratedResponseProfile, RootPinnedResponseProfileEvidence]:
    """R2-D/R2-E: independently re-verify and root-pin the completed run.

    `result.semantic_verification` is the producer's own R2-C computation;
    passing its root as `expected_raw_evidence_sha256` here makes
    `issue_root_pinned_response_profile_evidence` independently reconstruct
    and compare against it -- the same "caller supplies expected, callee
    reruns and compares" discipline R2-D requires, not a self-issued root.
    """

    assert result.profile_identity is not None and result.semantic_verification is not None
    exported = ledger.export_verified_lifecycle()
    bundle = ResponseProfileSemanticBundle(
        calibration_population=run_binding.population,
        warmup_role_manifest=run_binding.warmup_role_manifest,
        replay_schedule=run_binding.replay_schedule,
        run_binding=exported.run_binding,
        events=exported.events,
        opaque_evidence=exported.opaque_evidence,
        oracle_manifest=oracle_manifest,
        control=control,
    )
    expectation = ResponseProfileSemanticExpectation(
        profile_identity=result.profile_identity,
        expected_oracle_manifest_sha256=oracle_manifest.oracle_manifest_sha256,
    )
    capability = issue_root_pinned_response_profile_evidence(
        bundle=bundle,
        expectation=expectation,
        expected_raw_evidence_sha256=result.semantic_verification.raw_evidence_sha256,
    )
    profile = project_root_pinned_response_profile(
        capability=capability,
        expected_raw_evidence_sha256=capability.raw_evidence_sha256,
        expected_identity=result.profile_identity,
    )
    return profile, capability


def _manifest_document(
    *,
    evidence_status: str,
    collection_name: str,
    run_binding_sha256: str,
    control_sha256: str,
    result: ResponseProfileProducerResult,
    profile: CalibratedResponseProfile | None,
    capability: RootPinnedResponseProfileEvidence | None,
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema_version": "exp011-live-acquisition-manifest-v1",
        "evidence_status": evidence_status,
        "collection_name": collection_name,
        "run_binding_sha256": run_binding_sha256,
        "control_sha256": control_sha256,
        "producer_complete": result.complete,
        "producer_reason_codes": list(result.reason_codes),
        "closed_block_count": result.closed_block_count,
        "completed_position_count": result.completed_position_count,
        "warmup_search_calls": result.warmup_search_calls,
        "measured_search_calls": result.measured_search_calls,
        "profile_sha256": None,
        "raw_evidence_sha256": None,
        "root_pinned_capability_note": (
            "This manifest is NOT FreshResponseProfileEvidence and does not "
            "establish qualification, policy, admission, grant, activation, "
            "routing, or execution authority. Freshness binding against a "
            "verified-latest detector head is a separate, later step."
        ),
    }
    if profile is not None:
        document["profile_sha256"] = profile.profile_sha256
    if capability is not None:
        document["raw_evidence_sha256"] = capability.raw_evidence_sha256
    return document


def _read_json_document(path: Path, *, field: str) -> object:
    """Read and parse one artifact file as JSON; fail closed on any I/O or
    syntax problem before any governed reconstruction is even attempted."""

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise Exp011LiveAcquisitionError(f"{field}: cannot read {path}: {exc}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise Exp011LiveAcquisitionError(f"{field}: {path} is not valid JSON: {exc}") from exc


def load_control_artifact(path: Path) -> ResponseProfileControl:
    """Load and strictly reconstruct one `ResponseProfileControl` from a
    `--control-json` artifact file. Fails closed on any I/O, JSON-syntax, or
    governed-reconstruction problem -- never repairs or defaults a field."""

    document = _read_json_document(path, field="control_json")
    try:
        return response_profile_control_from_document(document)
    except ResponseProfileControlError as exc:
        raise Exp011LiveAcquisitionError(f"control_json: {path} is invalid: {exc}") from exc


def load_static_identity_artifact(path: Path) -> ResponseProfileStaticIdentity:
    """Load and strictly reconstruct one `ResponseProfileStaticIdentity` from
    a `--static-identity-json` artifact file. Fails closed on any I/O,
    JSON-syntax, or governed-reconstruction problem."""

    document = _read_json_document(path, field="static_identity_json")
    try:
        return response_profile_static_identity_from_document(document)
    except ResponseProfileSemanticError as exc:
        raise Exp011LiveAcquisitionError(f"static_identity_json: {path} is invalid: {exc}") from exc


def load_vector_material_artifact(path: Path) -> VerifiedResponseProfileVectorMaterial:
    """Load and strictly verify the supplemental
    `response-profile-vector-material-v1` artifact. Fails closed on any I/O,
    JSON-syntax, or verification problem, before any governed object is
    reconstructed from it."""

    document = _read_json_document(path, field="vector_material")
    try:
        return load_response_profile_vector_material(document)
    except ResponseProfileVectorMaterialError as exc:
        raise Exp011LiveAcquisitionError(f"vector_material: {path} is invalid: {exc}") from exc


def load_run_binding_artifact(
    path: Path, *, vector_material: VerifiedResponseProfileVectorMaterial
) -> ResponseProfileRunBinding:
    """Load one `--run-binding-json` document and reconstruct the governed
    `ResponseProfileRunBinding` from it plus the already-verified vector
    material. The reconstruction is rejected unless its own canonical document
    is byte-identical to the authoritative input document."""

    document = _read_json_document(path, field="run_binding_json")
    try:
        return response_profile_run_binding_from_document(
            document, vector_material=vector_material
        )
    except ResponseProfileVectorMaterialError as exc:
        raise Exp011LiveAcquisitionError(f"run_binding_json: {path} is invalid: {exc}") from exc


def load_oracle_manifest_artifact(
    path: Path, *, vector_material: VerifiedResponseProfileVectorMaterial
) -> ResponseProfileOracleManifest:
    """Load one `--oracle-manifest-json` document and reconstruct the governed
    `ResponseProfileOracleManifest` from it plus the already-verified vector
    material. The reconstruction is rejected unless its own canonical document
    is byte-identical to the authoritative input document."""

    document = _read_json_document(path, field="oracle_manifest_json")
    try:
        return response_profile_oracle_manifest_from_document(
            document, vector_material=vector_material
        )
    except ResponseProfileVectorMaterialError as exc:
        raise Exp011LiveAcquisitionError(f"oracle_manifest_json: {path} is invalid: {exc}") from exc


def _cross_validate_governed_inputs(
    *,
    run_binding: ResponseProfileRunBinding,
    static_identity: ResponseProfileStaticIdentity,
    control: ResponseProfileControl,
    oracle_manifest: ResponseProfileOracleManifest,
) -> None:
    """Mirror `ResponseProfileProducer.__init__`'s exact control / static
    identity / run-binding / oracle cross-checks, moved AHEAD of any Milvus
    client, ledger, or lifecycle STARTED construction.

    The producer re-runs these identical checks when it actually runs (defense
    in depth); performing them here is what guarantees an incompatible governed
    set causes zero Milvus interaction and zero output artifact. This invents
    no new governance -- every comparison is the one the producer already
    enforces.
    """

    try:
        verified_control = verify_response_profile_control(control)
    except (AttributeError, TypeError, ValueError) as exc:
        raise Exp011LiveAcquisitionError(f"control is invalid: {exc}") from exc
    if (
        verified_control.control_profile_sha256 != static_identity.control_profile_sha256
        or verified_control.calibration_population_sha256
        != run_binding.workload_manifest_sha256
        or verified_control.warmup_role_manifest_sha256
        != run_binding.warmup_role_manifest_sha256
        or verified_control.ordered_query_payload_sha256
        != run_binding.population.ordered_query_payload_sha256
        or verified_control.replay_schedule_sha256 != run_binding.replay_schedule_sha256
        or verified_control.environment_manifest_sha256
        != static_identity.environment_manifest_sha256
        or verified_control.source_revision != static_identity.source_revision
        or verified_control.stream_key.metric is not static_identity.metric
        or verified_control.stream_key.threshold_stratum
        != static_identity.threshold_stratum
        or verified_control.stream_key.data_identity != static_identity.data_identity
        or verified_control.stream_key.hnsw_binding_id
        != static_identity.hnsw_index_identity
    ):
        raise Exp011LiveAcquisitionError(
            "governed control differs from the run identity (control / static "
            "identity / run binding mismatch)"
        )
    if (
        oracle_manifest.workload_manifest_sha256 != run_binding.workload_manifest_sha256
        or len(oracle_manifest.records)
        != len(run_binding.population.calibration_role_manifest.members)
    ):
        raise Exp011LiveAcquisitionError(
            "oracle manifest differs from the run population"
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--milvus-uri", required=True)
    parser.add_argument("--collection-name", required=True)
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--metric", choices=[item.value for item in Metric], required=True)
    parser.add_argument("--ledger-path", type=Path, required=True)
    parser.add_argument("--run-binding-json", type=Path, required=True)
    parser.add_argument("--static-identity-json", type=Path, required=True)
    parser.add_argument("--control-json", type=Path, required=True)
    parser.add_argument("--oracle-manifest-json", type=Path, required=True)
    parser.add_argument(
        "--vector-material",
        type=Path,
        required=True,
        help=(
            "Supplemental response-profile-vector-material-v1 artifact carrying "
            "the raw canonical vector bytes (plus the verbatim role-manifest "
            "documents) needed to reconstruct the run binding and oracle "
            "manifest. Fully verified but non-authorizing: the governed "
            "run-binding/oracle digests remain the sole authority."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/exp-011/live"))
    parser.add_argument("--max-blocks", type=int, default=None)
    parser.add_argument("--etcd-container", default="milvus-etcd")
    parser.add_argument("--minio-container", default="milvus-minio")
    parser.add_argument(
        "--evidence-status",
        required=True,
        help=(
            "Explicit evidence-status label for the manifest this run produces. "
            "Never defaulted -- an operator running this for real is expected to "
            "pass PROSPECTIVE explicitly; this CLI never assumes it."
        ),
    )
    return parser


def run_exp011_live_acquisition_from_cli(
    args: argparse.Namespace,
    *,
    run_binding: ResponseProfileRunBinding,
    static_identity: ResponseProfileStaticIdentity,
    control: ResponseProfileControl,
    oracle_manifest: ResponseProfileOracleManifest,
) -> Exp011LiveAcquisitionResult:
    """The single real-operator acquisition seam.

    Reached only after `main` has fully loaded and cross-validated every
    governed artifact and the vector material. Builds the read-only Milvus
    client (PyMilvus stays in `response_profile_milvus_adapter`) and a Docker
    stack-health probe, then drives the unmodified `run_exp011_live_acquisition`.
    This repository never invokes this for real; tests patch this symbol to
    prove only a fully validated input set reaches it -- and that a malformed or
    mismatched set never does.
    """

    from .docker_health import DockerSocketHealthProbe

    client = build_response_profile_milvus_client(args.milvus_uri)
    stack_health_probe = DockerSocketHealthProbe(
        etcd_container=args.etcd_container,
        minio_container=args.minio_container,
    )
    return run_exp011_live_acquisition(
        client=client,
        stack_health_probe=stack_health_probe,
        collection_name=args.collection_name,
        dimensions=args.dimensions,
        metric=Metric(args.metric),
        ledger_path=args.ledger_path,
        run_binding=run_binding,
        static_identity=static_identity,
        control=control,
        oracle_manifest=oracle_manifest,
        output_dir=args.output_dir,
        evidence_status=args.evidence_status,
        max_blocks=args.max_blocks,
    )


def main(argv: list[str] | None = None) -> int:
    """Real operator entry point. NOT invoked by this repository's own code,
    tests, or any import of this module -- only an explicit, separately
    authorized operator command line reaches this function.

    Every governed artifact and the supplemental vector material is fully
    loaded, reconstructed, and cross-validated BEFORE any Milvus client,
    lifecycle ledger, or lifecycle STARTED is constructed. A malformed or
    mismatched input therefore causes zero Milvus interaction and zero output
    artifact. Only after all validation passes does control reach the single
    acquisition seam `run_exp011_live_acquisition_from_cli`.
    """

    args = _parser().parse_args(argv)
    if not isinstance(args.evidence_status, str) or not args.evidence_status:
        raise Exp011LiveAcquisitionError("--evidence-status must be a non-empty string")

    control = load_control_artifact(args.control_json)
    static_identity = load_static_identity_artifact(args.static_identity_json)
    vector_material = load_vector_material_artifact(args.vector_material)
    run_binding = load_run_binding_artifact(
        args.run_binding_json, vector_material=vector_material
    )
    oracle_manifest = load_oracle_manifest_artifact(
        args.oracle_manifest_json, vector_material=vector_material
    )
    _cross_validate_governed_inputs(
        run_binding=run_binding,
        static_identity=static_identity,
        control=control,
        oracle_manifest=oracle_manifest,
    )

    result = run_exp011_live_acquisition_from_cli(
        args,
        run_binding=run_binding,
        static_identity=static_identity,
        control=control,
        oracle_manifest=oracle_manifest,
    )
    print(result.manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
