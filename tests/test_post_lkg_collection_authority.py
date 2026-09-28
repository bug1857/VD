"""Focused adversarial coverage for ADR-025 collection authority."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

from tests.test_lkg_phase3_persistence import _authority as base_d1
from vdbench.canonical_serialization import (
    strict_canonical_digest,
    strict_canonical_json_bytes,
)
from vdbench.deployment_governance import (
    ensure_deployment_scope_directory,
    resolve_deployment_governance_scope,
)
from vdbench.exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from vdbench.exp010_v2_host import pin_dataset001_identity
from vdbench.lkg_phase3_persistence import LkgPhase3AuthorityReferenceStore
from vdbench.lkg_run_binding import LkgRunBinding
from vdbench.post_lkg_collection_authority import (
    POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN,
    PostLkgCollectionAuthority,
    PostLkgCollectionAuthorityError,
    PostLkgCollectionPrepareInput,
    VerifiedPostLkgCollectionAuthority,
    derive_post_lkg_campaign_root,
    derive_post_lkg_stream_id,
    load_post_lkg_collection_prepare_input,
    load_verified_post_lkg_collection_authority,
    materialize_post_lkg_collection_authority,
    prepare_post_lkg_collection_authority,
    verify_post_lkg_collection_authority_d2,
)


_DATASET = Path("artifacts/exp-001/dataset").resolve()
_TIMESTAMP = "2026-09-12T00:00:00.000000Z"


def _task_d1(*, identifier: int, data_identity: str):
    authority = base_d1(identifier)
    source = authority.run_binding
    binding = LkgRunBinding(
        run_id=source.run_id,
        producer_identity=source.producer_identity,
        search_configuration=source.search_configuration,
        collection_name="golden_hnsw",
        base_data_identity=data_identity,
        index_identity="golden-hnsw-binding-v1",
        qualification_dataset_id=source.qualification_dataset_id,
        qualification_dataset_version=source.qualification_dataset_version,
        qualification_manifest_sha256=source.qualification_manifest_sha256,
        qualification_query_role=source.qualification_query_role,
        qualification_query_id_array_sha256=source.qualification_query_id_array_sha256,
        qualification_ordered_query_ids_sha256=(
            source.qualification_ordered_query_ids_sha256
        ),
        qualification_query_array_sha256=source.qualification_query_array_sha256,
        qualification_expected_query_count=source.qualification_expected_query_count,
        environment_identity=source.environment_identity,
        source_revision=f"{identifier:040x}",
    )
    object.__setattr__(authority, "_run_binding", binding)
    evaluation = authority.checkpoint_c_evaluation
    evaluation.source_run_binding_sha256 = binding.sha256
    return authority


class PostLkgAuthorityCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.scope = resolve_deployment_governance_scope(
            canonical_root=self.root / "governance"
        )
        ensure_deployment_scope_directory(self.scope)
        self.dataset = pin_dataset001_identity(_DATASET)
        self.d1 = _task_d1(identifier=51, data_identity=self.dataset.data_identity)
        with LkgPhase3AuthorityReferenceStore(
            self.scope.lkg_authority_store_path
        ) as store:
            store.append(self.d1, persisted_at_utc=_TIMESTAMP)
            latest = store.load_verified_latest()
        assert latest is not None
        self.latest = latest
        self.prepare_input = PostLkgCollectionPrepareInput(
            campaign_id="golden-campaign-01",
            collection_execution_source_revision="1" * 40,
            consistency_level="Strong",
            dataset001_dir=str(_DATASET),
            detector_seed=7,
            etcd_container="golden-etcd",
            expected_d2_record_sha256=latest.canonical_record_digest,
            expected_d2_sequence=latest.sequence_number,
            expected_row_count=10_000,
            external_workload_producer_identity="golden-producer-v1",
            flat_binding_id="golden-flat-binding-v1",
            flat_collection_name="golden_flat",
            hnsw_binding_id="golden-hnsw-binding-v1",
            hnsw_collection_name="golden_hnsw",
            host_address="127.0.0.1",
            host_port=18_080,
            limit=100,
            milvus_container="golden-milvus",
            milvus_uri="http://127.0.0.1:19530",
            minio_container="golden-minio",
            range_filter=0.0,
            schema_version="vd-post-lkg-collection-prepare-input-v1",
            threshold_radius=191.85897352125554,
        )

    def prepare(self, **changes: object):
        value = replace(self.prepare_input, **changes)
        return prepare_post_lkg_collection_authority(
            value, governance_scope=self.scope
        )

    def persist_document(self, document: dict[str, object], digest: str | None = None):
        selected_digest = digest or strict_canonical_digest(
            POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN, document
        )
        root = Path(self.scope.collection_authorities_root)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(root, 0o700)
        campaign = document["campaign"]["campaign_id"]
        directory = root / campaign
        directory.mkdir(mode=0o700)
        path = directory / f"{selected_digest}.json"
        path.write_bytes(strict_canonical_json_bytes(document))
        os.chmod(path, 0o600)
        return load_verified_post_lkg_collection_authority(
            authority_sha256=selected_digest,
            governance_scope=self.scope,
        )


class PrepareTests(PostLkgAuthorityCase):
    def test_prepare_input_loader_requires_exact_canonical_closed_shape(self) -> None:
        document = {
            field: getattr(self.prepare_input, field)
            for field in self.prepare_input.__dataclass_fields__
        }
        path = self.root / "prepare.json"
        path.write_bytes(strict_canonical_json_bytes(document))
        self.assertEqual(
            load_post_lkg_collection_prepare_input(path), self.prepare_input
        )
        for mutation in ("missing", "extra"):
            changed = dict(document)
            if mutation == "missing":
                changed.pop("database_name", None)
                changed.pop("campaign_id")
            else:
                changed["unexpected"] = "value"
            path.write_bytes(strict_canonical_json_bytes(changed))
            with self.subTest(mutation=mutation), self.assertRaises(
                PostLkgCollectionAuthorityError
            ):
                load_post_lkg_collection_prepare_input(path)

    def test_exact_latest_d2_is_accepted_and_prepare_is_deterministic(self) -> None:
        d2_before = Path(self.scope.lkg_authority_store_path).read_bytes()
        first = self.prepare()
        second = self.prepare()
        self.assertEqual(first, second)
        self.assertEqual(
            Path(self.scope.lkg_authority_store_path).read_bytes(), d2_before
        )
        self.assertFalse(Path(self.scope.collection_authorities_root).exists())
        self.assertFalse(Path(self.scope.campaigns_root).exists())

    def test_golden_body_bytes_and_digest_use_only_strict_helpers(self) -> None:
        authority = self.prepare()
        reference = self.latest.reference
        configuration = Exp010ServingConfiguration(
            metric=self.d1.metric,
            threshold_stratum=self.d1.threshold_stratum,
            threshold_radius=self.prepare_input.threshold_radius,
            range_filter=0.0,
            limit=100,
            served_ef=400,
            dimensions=self.dataset.dimensions,
            consistency_level="Strong",
        )
        expected = {
            "campaign": {
                "campaign_id": "golden-campaign-01",
                "campaign_root": str(
                    derive_post_lkg_campaign_root(
                        governance_scope=self.scope,
                        campaign_id="golden-campaign-01",
                    )
                ),
                "stream_id": derive_post_lkg_stream_id(
                    d2_record_sha256=reference.canonical_record_digest,
                    campaign_id="golden-campaign-01",
                ),
            },
            "canonical_lkg": {
                "checkpoint_c_sha256": reference.canonical_evaluation_digest,
                "d2_record_sha256": reference.canonical_record_digest,
                "d2_sequence": 0,
                "deployment_identity": self.scope.deployment_identity,
                "deployment_namespace_sha256": self.scope.namespace_digest,
                "environment_identity": reference.environment_identity,
                "evaluated_ef": 400,
                "lkg_execution_source_revision": reference.source_revision,
                "source_run_binding_sha256": reference.source_run_binding_sha256,
                "source_run_id": reference.source_run_id,
            },
            "collection_execution_source_revision": "1" * 40,
            "containers": {
                "etcd_container": "golden-etcd",
                "milvus_container": "golden-milvus",
                "minio_container": "golden-minio",
            },
            "dataset": {
                "base_vectors_sha256": self.dataset.base_vectors_sha256,
                "data_identity": self.dataset.data_identity,
                "dataset001_dir": str(_DATASET),
                "generation_manifest_sha256": self.dataset.generation_manifest_sha256,
                "version": self.dataset.version,
            },
            "detector": {"seed": 7},
            "endpoint": {"host_address": "127.0.0.1", "host_port": 18_080},
            "milvus": {
                "database_name": "default",
                "flat_collection_name": "golden_flat",
                "hnsw_collection_name": "golden_hnsw",
                "index_name": "vector_index",
                "uri": "http://127.0.0.1:19530",
            },
            "search_budget": {
                "flat_shadow_positions": 600,
                "foreground_positions": 600,
                "hnsw_sentinel_positions": 600,
                "milvus_write_operations": 0,
            },
            "schema_version": "vd-post-lkg-collection-authority-v1",
            "serialization_contract": "vd-canonical-json-v2",
            "serving": {
                "configuration_identity": derive_serving_configuration_identity(
                    configuration
                ),
                "consistency_level": "Strong",
                "dimensions": self.dataset.dimensions,
                "flat_binding_id": "golden-flat-binding-v1",
                "hnsw_binding_id": "golden-hnsw-binding-v1",
                "limit": 100,
                "metric": "L2",
                "range_filter": 0.0,
                "served_ef": 400,
                "threshold_radius": 191.85897352125554,
                "threshold_stratum": "target-075",
            },
            "target": {
                "expected_row_count": 10_000,
                "hnsw_ef_construction": 200,
                "hnsw_m": 16,
            },
            "window_plan": {
                "complete_windows": 3,
                "current_window_sequences": [1, 2],
                "first_eligible_decision_sequence": 2,
                "queries_per_window": 200,
                "reference_window_sequence": 0,
                "sentinel_ef": 100,
                "served_ef": 400,
                "target_source_records": 600,
                "trace_count": 4,
                "trace_size": 50,
            },
            "workload_producer": {
                "external_workload_producer_identity": "golden-producer-v1"
            },
        }
        self.assertEqual(authority.canonical_bytes, strict_canonical_json_bytes(expected))
        self.assertEqual(
            authority.authority_sha256,
            strict_canonical_digest(
                POST_LKG_COLLECTION_AUTHORITY_DIGEST_DOMAIN, expected
            ),
        )
        self.assertTrue(authority.canonical_bytes.endswith(b"\n"))

    def test_wrong_d2_sequence_and_digest_are_refused(self) -> None:
        for changes, code in (
            ({"expected_d2_sequence": 1}, "POST_LKG_D2_SEQUENCE_MISMATCH"),
            (
                {"expected_d2_record_sha256": "f" * 64},
                "POST_LKG_D2_RECORD_DIGEST_MISMATCH",
            ),
        ):
            with self.subTest(code=code), self.assertRaises(
                PostLkgCollectionAuthorityError
            ) as raised:
                self.prepare(**changes)
            self.assertEqual(raised.exception.code, code)

    def test_lkg_and_collection_revisions_may_differ(self) -> None:
        document = self.prepare().document
        self.assertNotEqual(
            document["canonical_lkg"]["lkg_execution_source_revision"],
            document["collection_execution_source_revision"],
        )

    def test_wrong_hnsw_and_data_descendants_are_refused(self) -> None:
        cases = (
            ({"hnsw_collection_name": "different_hnsw"}, "COLLECTION"),
            ({"hnsw_binding_id": "different-binding"}, "INDEX"),
        )
        for changes, fragment in cases:
            with self.subTest(fragment=fragment), self.assertRaises(
                PostLkgCollectionAuthorityError
            ) as raised:
                self.prepare(**changes)
            self.assertIn(fragment, raised.exception.code)

    def test_strict_types_ranges_and_identifiers_refuse(self) -> None:
        class TextSubclass(str):
            pass

        cases = (
            {"detector_seed": True},
            {"detector_seed": 2**32},
            {"host_port": 0},
            {"host_address": "0.0.0.0"},
            {"campaign_id": "Upper"},
            {"external_workload_producer_identity": " producer"},
            {"range_filter": 0},
            {"range_filter": float("nan")},
            {"collection_execution_source_revision": "revision"},
            {
                "schema_version": TextSubclass(
                    "vd-post-lkg-collection-prepare-input-v1"
                )
            },
            {"host_address": TextSubclass("127.0.0.1")},
        )
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(
                PostLkgCollectionAuthorityError
            ):
                replace(self.prepare_input, **changes)

    def test_persisted_authority_refuses_non_contract_json_types(self) -> None:
        cases = (
            (("search_budget", "milvus_write_operations"), False),
            (("workload_producer", "external_workload_producer_identity"), None),
        )
        for index, (path, value) in enumerate(cases):
            document = deepcopy(
                self.prepare(campaign_id=f"non-contract-type-{index}").document
            )
            document[path[0]][path[1]] = value
            with self.subTest(path=path), self.assertRaises(
                PostLkgCollectionAuthorityError
            ):
                self.persist_document(document)

    def test_private_constructors_refuse_arbitrary_creation(self) -> None:
        with self.assertRaises(TypeError):
            PostLkgCollectionAuthority()
        with self.assertRaises(TypeError):
            VerifiedPostLkgCollectionAuthority()


class MaterializationTests(PostLkgAuthorityCase):
    def test_materialize_and_exact_replay_are_private_and_immutable(self) -> None:
        authority = self.prepare()
        verified = materialize_post_lkg_collection_authority(
            authority, governance_scope=self.scope
        )
        campaigns_root = Path(self.scope.campaigns_root)
        self.assertTrue(campaigns_root.is_dir())
        self.assertEqual(stat.S_IMODE(os.lstat(campaigns_root).st_mode), 0o700)
        campaigns_root.rmdir()
        replay = materialize_post_lkg_collection_authority(
            authority, governance_scope=self.scope
        )
        self.assertEqual(replay.canonical_bytes, authority.canonical_bytes)
        self.assertEqual(verified.path, replay.path)
        self.assertEqual(stat.S_IMODE(os.lstat(verified.path).st_mode), 0o600)
        self.assertEqual(
            stat.S_IMODE(os.lstat(verified.path.parent).st_mode), 0o700
        )
        self.assertFalse(campaigns_root.exists())
        self.assertEqual(
            verify_post_lkg_collection_authority_d2(
                replay, governance_scope=self.scope
            ).canonical_record_digest,
            self.latest.canonical_record_digest,
        )

    def test_same_campaign_different_authority_is_refused(self) -> None:
        first = self.prepare()
        materialize_post_lkg_collection_authority(first, governance_scope=self.scope)
        second = self.prepare(external_workload_producer_identity="other-producer")
        with self.assertRaises(PostLkgCollectionAuthorityError) as raised:
            materialize_post_lkg_collection_authority(
                second, governance_scope=self.scope
            )
        self.assertEqual(raised.exception.code, "POST_LKG_AUTHORITY_CAMPAIGN_CONFLICT")

    def test_same_digest_foreign_bytes_and_hard_links_are_refused(self) -> None:
        verified = materialize_post_lkg_collection_authority(
            self.prepare(), governance_scope=self.scope
        )
        verified.path.write_bytes(b"{}\n")
        os.chmod(verified.path, 0o600)
        with self.assertRaises(PostLkgCollectionAuthorityError):
            load_verified_post_lkg_collection_authority(
                authority_sha256=verified.authority_sha256,
                governance_scope=self.scope,
            )

        verified.path.write_bytes(verified.canonical_bytes)
        linked = self.root / "linked-authority.json"
        os.link(verified.path, linked)
        with self.assertRaises(PostLkgCollectionAuthorityError) as raised:
            load_verified_post_lkg_collection_authority(
                authority_sha256=verified.authority_sha256,
                governance_scope=self.scope,
            )
        self.assertEqual(raised.exception.code, "POST_LKG_AUTHORITY_FILE_UNSAFE")

    def test_authority_modes_are_exact_not_merely_private(self) -> None:
        verified = materialize_post_lkg_collection_authority(
            self.prepare(), governance_scope=self.scope
        )
        for path, mode in (
            (verified.path, 0o400),
            (verified.path.parent, 0o500),
            (verified.path.parent.parent, 0o755),
        ):
            original = stat.S_IMODE(os.lstat(path).st_mode)
            os.chmod(path, mode)
            try:
                with self.subTest(path=path, mode=oct(mode)), self.assertRaises(
                    PostLkgCollectionAuthorityError
                ):
                    load_verified_post_lkg_collection_authority(
                        authority_sha256=verified.authority_sha256,
                        governance_scope=self.scope,
                    )
            finally:
                os.chmod(path, original)

    def test_old_authority_is_rejected_after_new_latest_d2(self) -> None:
        verified = materialize_post_lkg_collection_authority(
            self.prepare(), governance_scope=self.scope
        )
        later = _task_d1(identifier=52, data_identity=self.dataset.data_identity)
        with LkgPhase3AuthorityReferenceStore(
            self.scope.lkg_authority_store_path
        ) as store:
            store.append(later, persisted_at_utc="2026-09-12T00:01:00.000000Z")
        with self.assertRaises(PostLkgCollectionAuthorityError) as raised:
            verify_post_lkg_collection_authority_d2(
                verified, governance_scope=self.scope
            )
        self.assertEqual(raised.exception.code, "POST_LKG_D2_PROJECTION_MISMATCH")

    def test_tampered_lkg_projection_database_and_namespace_are_refused(self) -> None:
        cases = (
            ("source-run", ("canonical_lkg", "source_run_id"), "other-run"),
            (
                "run-binding",
                ("canonical_lkg", "source_run_binding_sha256"),
                "f" * 64,
            ),
            (
                "environment",
                ("canonical_lkg", "environment_identity"),
                "other-environment",
            ),
            (
                "deployment",
                ("canonical_lkg", "deployment_identity"),
                "ENV-OTHER",
            ),
            (
                "namespace",
                ("canonical_lkg", "deployment_namespace_sha256"),
                "f" * 64,
            ),
            ("database", ("milvus", "database_name"), "implicit-default"),
            ("hnsw", ("milvus", "hnsw_collection_name"), "other_hnsw"),
            ("data", ("dataset", "data_identity"), "other-data"),
        )
        for index, (label, path, value) in enumerate(cases):
            with self.subTest(case=label):
                document = deepcopy(
                    self.prepare(campaign_id=f"tamper-{index}").document
                )
                document[path[0]][path[1]] = value
                if label in {"deployment", "namespace", "database"}:
                    with self.assertRaises(PostLkgCollectionAuthorityError):
                        self.persist_document(document)
                else:
                    verified = self.persist_document(document)
                    with self.assertRaises(PostLkgCollectionAuthorityError):
                        verify_post_lkg_collection_authority_d2(
                            verified, governance_scope=self.scope
                        )

    def test_missing_malformed_and_wrong_digest_authorities_are_refused(self) -> None:
        with self.assertRaises(PostLkgCollectionAuthorityError):
            load_verified_post_lkg_collection_authority(
                authority_sha256="f" * 64, governance_scope=self.scope
            )

        document = self.prepare(campaign_id="wrong-digest").document
        with self.assertRaises(PostLkgCollectionAuthorityError):
            self.persist_document(document, digest="f" * 64)

        root = Path(self.scope.collection_authorities_root)
        directory = root / "malformed"
        directory.mkdir(mode=0o700)
        path = directory / f'{"e" * 64}.json'
        path.write_bytes(b"{}\n")
        os.chmod(path, 0o600)
        with self.assertRaises(PostLkgCollectionAuthorityError):
            load_verified_post_lkg_collection_authority(
                authority_sha256="e" * 64, governance_scope=self.scope
            )

    def test_campaign_and_lkg_stream_namespaces_are_isolated(self) -> None:
        first = materialize_post_lkg_collection_authority(
            self.prepare(campaign_id="isolated-01"), governance_scope=self.scope
        )
        second = materialize_post_lkg_collection_authority(
            self.prepare(campaign_id="isolated-02"), governance_scope=self.scope
        )
        self.assertNotEqual(first.path.parent, second.path.parent)
        self.assertNotEqual(
            first.document["campaign"]["stream_id"],
            second.document["campaign"]["stream_id"],
        )
        self.assertNotEqual(
            derive_post_lkg_stream_id(
                d2_record_sha256="a" * 64, campaign_id="same-campaign"
            ),
            derive_post_lkg_stream_id(
                d2_record_sha256="b" * 64, campaign_id="same-campaign"
            ),
        )

    def test_unauthorized_deployment_scope_is_refused(self) -> None:
        foreign = resolve_deployment_governance_scope(
            canonical_root=self.root / "foreign", deployment_identity="ENV-OTHER"
        )
        with self.assertRaises(PostLkgCollectionAuthorityError):
            prepare_post_lkg_collection_authority(
                self.prepare_input, governance_scope=foreign
            )


if __name__ == "__main__":
    unittest.main()
