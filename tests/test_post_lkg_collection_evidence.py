"""Offline end-to-end proof for ADR-025 descendant detector evidence."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import fields, replace
from io import StringIO
from pathlib import Path
from unittest import mock

from tests.test_post_lkg_collection_authority import PostLkgAuthorityCase
from tests.test_shadow_extraction import _identity, _query
from vdbench.canonical_serialization import (
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from vdbench.config import IndexTrack, Metric
from vdbench.exp010_gate_a_operator import (
    GATE_A_EVIDENCE_FILENAME,
    GATE_A_EVIDENCE_SUBDIRECTORY,
    Exp010GateAObservation,
    Exp010GateAOperands,
    build_gate_a_evidence,
    initialize_v5_campaign,
    main as gate_a_main,
)
from vdbench.exp010_gate_c_operator import (
    _load_post_lkg_operands as load_gate_c_post_lkg_operands,
    run_gate_c_execute_from_cli,
)
from vdbench.exp010_live_runner import (
    Exp010LiveRunner,
    Exp010OperatorConfiguration,
    build_environment_manifest_sha256,
)
from vdbench.host_observation import RangeQueryRequest, ServedQueryOutcome
from vdbench.lkg_phase3_binding import bind_lkg_phase3_authority
from vdbench.milvus_actuation import ShadowAuditTrace
from vdbench.policy import PolicyMode, evaluate_post_lkg_tuning_policy
from vdbench.post_lkg_collection_authority import (
    materialize_post_lkg_collection_authority,
)
from vdbench.post_lkg_collection_evidence import (
    PostLkgCollectionEvidenceError,
    VerifiedPostLkgDetectorEvidence,
    load_verified_post_lkg_detector_evidence,
)


class _Serving:
    def execute(self, _request: RangeQueryRequest) -> ServedQueryOutcome:
        return ServedQueryOutcome(True, False, 1, 1.0)


class _ShadowCapture:
    def __init__(self, document: dict[str, object]) -> None:
        self.document = document

    def capture(self, sources, *, trace_sequence_index: int) -> ShadowAuditTrace:
        del trace_sequence_index
        serving = self.document["serving"]
        dataset = self.document["dataset"]
        return ShadowAuditTrace(
            metric=Metric(serving["metric"]),
            threshold_stratum=serving["threshold_stratum"],
            candidate_ef=serving["served_ef"],
            last_known_good_ef=serving["served_ef"],
            sentinel_ef=self.document["window_plan"]["sentinel_ef"],
            configuration_identity=serving["configuration_identity"],
            data_identity=dataset["data_identity"],
            flat_identity=_identity(
                IndexTrack.FLAT,
                Metric.L2,
                binding_id=serving["flat_binding_id"],
            ),
            hnsw_identity=_identity(
                IndexTrack.HNSW,
                Metric.L2,
                binding_id=serving["hnsw_binding_id"],
            ),
            queries=tuple(_query(int(item.query_id), Metric.L2) for item in sources),
            complete=True,
        )


class PostLkgEvidenceCase(PostLkgAuthorityCase):
    def setUp(self) -> None:
        super().setUp()
        self.prepare_input = replace(self.prepare_input, threshold_radius=2.0)
        self.verified_authority = materialize_post_lkg_collection_authority(
            self.prepare(), governance_scope=self.scope
        )
        self._initialize_gate_a(self.verified_authority)
        self._seed_complete_campaign(self.verified_authority)

    def _initialize_gate_a(self, authority) -> None:
        document = authority.document
        campaign = document["campaign"]
        serving = document["serving"]
        dataset = document["dataset"]
        milvus = document["milvus"]
        containers = document["containers"]
        observed_at = "2026-09-12T01:00:00Z"
        environment_observation = {
            "milvus_uri": milvus["uri"],
            "deployment_identity": document["canonical_lkg"]["deployment_identity"],
            "flat_collection_name": milvus["flat_collection_name"],
            "hnsw_collection_name": milvus["hnsw_collection_name"],
            "metric": serving["metric"],
            "threshold_stratum": serving["threshold_stratum"],
            "dimensions": serving["dimensions"],
            "flat_index_identity": serving["flat_binding_id"],
            "hnsw_index_identity": serving["hnsw_binding_id"],
            "data_identity": dataset["data_identity"],
            "source_revision": document["collection_execution_source_revision"],
            "served_ef": serving["served_ef"],
            "observed_at_utc": observed_at,
        }
        environment_sha256 = build_environment_manifest_sha256(
            environment_observation
        )
        operands = Exp010GateAOperands(
            deployment_identity=document["canonical_lkg"]["deployment_identity"],
            stream_id=campaign["stream_id"],
            campaign_root=Path(campaign["campaign_root"]),
            milvus_uri=milvus["uri"],
            flat_collection_name=milvus["flat_collection_name"],
            hnsw_collection_name=milvus["hnsw_collection_name"],
            metric=Metric(serving["metric"]),
            threshold_stratum=serving["threshold_stratum"],
            threshold_radius=serving["threshold_radius"],
            range_filter=serving["range_filter"],
            limit=serving["limit"],
            served_ef=serving["served_ef"],
            dimensions=serving["dimensions"],
            consistency_level=serving["consistency_level"],
            configuration_identity=serving["configuration_identity"],
            flat_binding_id=serving["flat_binding_id"],
            hnsw_binding_id=serving["hnsw_binding_id"],
            source_revision=document["collection_execution_source_revision"],
            expected_row_count=document["target"]["expected_row_count"],
            hnsw_m=document["target"]["hnsw_m"],
            hnsw_ef_construction=document["target"]["hnsw_ef_construction"],
            dataset001_dir=Path(dataset["dataset001_dir"]),
            etcd_container=containers["etcd_container"],
            minio_container=containers["minio_container"],
            milvus_container=containers["milvus_container"],
            verified_post_lkg_collection_authority=authority,
            database_name=milvus["database_name"],
        )
        live = {
            "collection_name": milvus["flat_collection_name"],
            "index_name": milvus["index_name"],
            "index_type": "FLAT",
            "metric_type": serving["metric"],
            "row_count": document["target"]["expected_row_count"],
            "dimensions": serving["dimensions"],
            "indexed_rows": document["target"]["expected_row_count"],
            "pending_index_rows": 0,
            "index_state": "Finished",
            "load_state": "Loaded",
        }
        observation = Exp010GateAObservation(
            observed_at_utc=observed_at,
            data_identity=dataset["data_identity"],
            generation_manifest_sha256=dataset["generation_manifest_sha256"],
            base_vectors_sha256=dataset["base_vectors_sha256"],
            dataset_version=dataset["version"],
            flat=live,
            hnsw={
                **live,
                "collection_name": milvus["hnsw_collection_name"],
                "index_type": "HNSW",
                "M": document["target"]["hnsw_m"],
                "efConstruction": document["target"]["hnsw_ef_construction"],
            },
            containers={
                name: {"container_name": value}
                for name, value in (
                    ("etcd", containers["etcd_container"]),
                    ("minio", containers["minio_container"]),
                    ("milvus", containers["milvus_container"]),
                )
            },
            environment_manifest_sha256=environment_sha256,
            environment_observation=environment_observation,
        )
        with mock.patch(
            "vdbench.deployment_governance.canonical_deployment_governance_scope",
            return_value=self.scope,
        ):
            initialize_v5_campaign(operands, observation)

    def _configuration(self, authority) -> Exp010OperatorConfiguration:
        document = authority.document
        serving = document["serving"]
        milvus = document["milvus"]
        return Exp010OperatorConfiguration(
            milvus_uri=milvus["uri"],
            flat_collection_name=milvus["flat_collection_name"],
            hnsw_collection_name=milvus["hnsw_collection_name"],
            metric=Metric(serving["metric"]),
            threshold_stratum=serving["threshold_stratum"],
            threshold_radius=serving["threshold_radius"],
            served_ef=serving["served_ef"],
            detector_seed=document["detector"]["seed"],
            stream_id=document["campaign"]["stream_id"],
            configuration_identity=serving["configuration_identity"],
            flat_binding_id=serving["flat_binding_id"],
            hnsw_binding_id=serving["hnsw_binding_id"],
            source_revision=document["collection_execution_source_revision"],
            environment_manifest_sha256=build_environment_manifest_sha256(
                {
                    "milvus_uri": milvus["uri"],
                    "deployment_identity": document["canonical_lkg"][
                        "deployment_identity"
                    ],
                    "flat_collection_name": milvus["flat_collection_name"],
                    "hnsw_collection_name": milvus["hnsw_collection_name"],
                    "metric": serving["metric"],
                    "threshold_stratum": serving["threshold_stratum"],
                    "dimensions": serving["dimensions"],
                    "flat_index_identity": serving["flat_binding_id"],
                    "hnsw_index_identity": serving["hnsw_binding_id"],
                    "data_identity": document["dataset"]["data_identity"],
                    "source_revision": document[
                        "collection_execution_source_revision"
                    ],
                    "served_ef": serving["served_ef"],
                    "observed_at_utc": "2026-09-12T01:00:00Z",
                }
            ),
            store_root=Path(document["campaign"]["campaign_root"]) / "stores",
            dataset001_dir=Path(document["dataset"]["dataset001_dir"]),
            exp010_output_dir=Path(document["campaign"]["campaign_root"])
            / "exp010",
        )

    def _seed_complete_campaign(self, authority) -> None:
        tick = 0

        def shadow_clock() -> str:
            nonlocal tick
            tick += 1
            return f"2026-09-12T02:{tick // 60:02d}:{tick % 60:02d}Z"

        runner = Exp010LiveRunner(
            configuration=self._configuration(authority),
            serving_executor=_Serving(),
            shadow_capture_executor=_ShadowCapture(authority.document),
            clock=lambda: "2026-09-12T02:00:00Z",
            shadow_captured_at_clock=shadow_clock,
        )
        try:
            for query_id in range(600):
                runner.serve(
                    RangeQueryRequest(
                        query_id,
                        runner.composition.stream_key,
                        (float(query_id + 1), 1.0),
                        2.0,
                        0.0,
                        100,
                        400,
                    )
                )
            self.assertEqual(
                tuple(item.window_sequence for item in runner.process_ready_windows()),
                (0, 1, 2),
            )
        finally:
            runner.close()

    def load(self, authority=None) -> VerifiedPostLkgDetectorEvidence:
        selected = authority or self.verified_authority
        pair = bind_lkg_phase3_authority(
            authority=self.d1, verified_latest_reference=self.latest
        )
        with mock.patch(
            "vdbench.deployment_governance.canonical_deployment_governance_scope",
            return_value=self.scope,
        ):
            return load_verified_post_lkg_detector_evidence(
                authority_sha256=selected.authority_sha256,
                lkg_authority_pair=pair,
            )


class VerifiedEvidenceTests(PostLkgEvidenceCase):
    def test_completed_gate_a_is_read_only_and_reopens_no_runtime(self) -> None:
        with mock.patch(
            "vdbench.deployment_governance.canonical_deployment_governance_scope",
            return_value=self.scope,
        ), mock.patch(
            "vdbench.v2_milvus_shadow_capture.build_readonly_milvus_client"
        ) as client, redirect_stdout(StringIO()):
            result = gate_a_main(
                [
                    "--expect-collection-authority-sha256",
                    self.verified_authority.authority_sha256,
                    "--mode",
                    "execute",
                    "--confirm-post-lkg-gate-a",
                ]
            )
        self.assertEqual(result, 0)
        client.assert_not_called()

    def test_complete_descendant_chain_loads_without_mutating_d2(self) -> None:
        before = Path(self.scope.lkg_authority_store_path).read_bytes()
        evidence = self.load()
        self.assertIsInstance(evidence, VerifiedPostLkgDetectorEvidence)
        self.assertEqual(
            evidence.collection_authority.authority_sha256,
            self.verified_authority.authority_sha256,
        )
        self.assertEqual(evidence.verified_real_detector_head.head.window_sequence, 2)
        self.assertEqual(Path(self.scope.lkg_authority_store_path).read_bytes(), before)

    def test_completed_gate_c_is_read_only_and_reopens_no_runtime(self) -> None:
        with mock.patch(
            "vdbench.deployment_governance.canonical_deployment_governance_scope",
            return_value=self.scope,
        ):
            operands = load_gate_c_post_lkg_operands(
                self.verified_authority.authority_sha256
            )
            with mock.patch(
                "vdbench.v2_milvus_shadow_capture.build_readonly_milvus_client"
            ) as client:
                self.assertEqual(run_gate_c_execute_from_cli(operands), ())
        client.assert_not_called()

    def test_historical_gate_a_manifest_is_rejected(self) -> None:
        path = (
            Path(self.verified_authority.document["campaign"]["campaign_root"])
            / GATE_A_EVIDENCE_SUBDIRECTORY
            / GATE_A_EVIDENCE_FILENAME
        )
        os.chmod(path, 0o600)
        evidence = decode_strict_canonical_json(path.read_bytes())
        evidence.pop("post_lkg_collection_authority_sha256")
        body = {key: value for key, value in evidence.items() if key != "evidence_sha256"}
        evidence["evidence_sha256"] = strict_canonical_digest(
            b"VD::EXP010_GATE_A_EVIDENCE::V1\x00", body
        )
        path.write_bytes(strict_canonical_json_bytes(evidence))
        os.chmod(path, 0o400)
        with self.assertRaises(PostLkgCollectionEvidenceError):
            self.load()

    def test_cross_campaign_source_shadow_and_window_evidence_is_rejected(self) -> None:
        second = materialize_post_lkg_collection_authority(
            self.prepare(campaign_id="golden-campaign-02"),
            governance_scope=self.scope,
        )
        self._initialize_gate_a(second)
        first_root = Path(self.verified_authority.document["campaign"]["campaign_root"])
        second_root = Path(second.document["campaign"]["campaign_root"])
        shutil.copytree(first_root / "stores", second_root / "stores")
        with self.assertRaises(PostLkgCollectionEvidenceError):
            self.load(second)

    def test_wrong_detector_head_and_attestation_fail_closed(self) -> None:
        stores = Path(self.verified_authority.document["campaign"]["campaign_root"])
        for name in ("v2_detector.sqlite3", "v2_attestation.sqlite3"):
            with self.subTest(store=name), tempfile.TemporaryDirectory() as raw:
                copy = Path(raw) / "stores"
                shutil.copytree(stores / "stores", copy)
                path = copy / name
                path.write_bytes(b"not a verified sqlite store")
                original = stores / "stores"
                moved = stores / "stores-original"
                original.rename(moved)
                copy.rename(original)
                try:
                    with self.assertRaises(PostLkgCollectionEvidenceError):
                        self.load()
                finally:
                    original.rename(copy)
                    moved.rename(original)

    def test_incomplete_campaign_has_no_policy_evidence(self) -> None:
        stores = Path(self.verified_authority.document["campaign"]["campaign_root"])
        finalization = stores / "stores" / "v2_window_finalization.sqlite3"
        finalization.unlink()
        with self.assertRaises(PostLkgCollectionEvidenceError) as raised:
            self.load()
        self.assertEqual(raised.exception.code, "POST_LKG_EVIDENCE_STORE_GROUP_INCOMPLETE")

    def test_policy_delegates_only_after_exact_evidence_binding(self) -> None:
        evidence = self.load()
        sentinel = object()
        with mock.patch("vdbench.policy.evaluate_tuning_policy", return_value=sentinel) as pure:
            result = evaluate_post_lkg_tuning_policy(
                evidence,
                current_ef=400,
                response_estimates={},
                pre_action=mock.sentinel.pre_action,
                canary_observation=None,
                qualification_windows=None,
                mode=PolicyMode.DRY_RUN,
                audit_id="post-lkg-policy-test",
            )
        self.assertIs(result, sentinel)
        self.assertIs(pure.call_args.args[0], evidence.drift_decision)
        self.assertEqual(pure.call_args.kwargs["threshold_stratum"], "target-075")
        self.assertIs(pure.call_args.kwargs["lkg_authority"], evidence.lkg_authority_pair)

    def test_policy_refuses_wrong_authority_detector_evidence(self) -> None:
        evidence = self.load()
        second = materialize_post_lkg_collection_authority(
            self.prepare(campaign_id="golden-campaign-02"),
            governance_scope=self.scope,
        )
        forged = object.__new__(VerifiedPostLkgDetectorEvidence)
        for field in fields(VerifiedPostLkgDetectorEvidence):
            object.__setattr__(
                forged,
                field.name,
                second if field.name == "collection_authority" else getattr(evidence, field.name),
            )
        with self.assertRaisesRegex(ValueError, "authority binding mismatch"):
            evaluate_post_lkg_tuning_policy(
                forged,
                current_ef=400,
                response_estimates={},
                pre_action=mock.sentinel.pre_action,
                canary_observation=None,
                qualification_windows=None,
                mode=PolicyMode.DRY_RUN,
                audit_id="wrong-authority",
            )

    def test_public_constructor_and_missing_authority_are_refused(self) -> None:
        with self.assertRaises(TypeError):
            VerifiedPostLkgDetectorEvidence()
        with mock.patch(
            "vdbench.deployment_governance.canonical_deployment_governance_scope",
            return_value=self.scope,
        ), self.assertRaises(PostLkgCollectionEvidenceError):
            load_verified_post_lkg_detector_evidence(
                authority_sha256="f" * 64,
                lkg_authority_pair=bind_lkg_phase3_authority(
                    authority=self.d1, verified_latest_reference=self.latest
                ),
            )


if __name__ == "__main__":
    unittest.main()
