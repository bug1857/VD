"""Focused adversarial tests for the neutral Phase-3 authority pair."""

from __future__ import annotations

import ast
from dataclasses import fields, replace
from pathlib import Path
import tempfile
import unittest

import vdbench.lkg_phase3_binding as binding_module
from vdbench.config import Metric
from vdbench.lkg_phase3_authority import LkgPhase3Authority
from vdbench.lkg_phase3_binding import (
    LkgPhase3AuthorityPair,
    bind_lkg_phase3_authority,
)
from vdbench.lkg_phase3_persistence import (
    LkgPhase3AuthorityReferenceStore,
    PersistedLkgPhase3AuthorityReference,
    VerifiedLatestLkgPhase3AuthorityReference,
)
from tests.test_lkg_phase3_persistence import _authority


_TIMESTAMP = "2026-08-08T16:00:00.000000Z"
_D2_METADATA_FIELDS = {
    "record_schema_version",
    "sequence_number",
    "persisted_at_utc",
    "previous_record_digest",
    "canonical_record_digest",
}


def _forge_verified_latest(
    reference: PersistedLkgPhase3AuthorityReference,
) -> VerifiedLatestLkgPhase3AuthorityReference:
    value = object.__new__(VerifiedLatestLkgPhase3AuthorityReference)
    object.__setattr__(value, "_reference", reference)
    return value


class LkgPhase3BindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        self.authority = _authority(41)
        with LkgPhase3AuthorityReferenceStore(self.root / "phase3.db") as store:
            self.persisted = store.append(
                self.authority,
                persisted_at_utc=_TIMESTAMP,
            ).reference
            latest = store.load_verified_latest()
        assert latest is not None
        self.verified_latest = latest

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def test_valid_exact_bind_returns_private_immutable_pair(self) -> None:
        pair = bind_lkg_phase3_authority(
            authority=self.authority,
            verified_latest_reference=self.verified_latest,
        )

        self.assertIs(type(pair), LkgPhase3AuthorityPair)
        self.assertIs(pair.authority, self.authority)
        self.assertIs(pair.verified_latest_reference, self.verified_latest)
        with self.assertRaises((AttributeError, TypeError)):
            pair._authority = self.authority  # type: ignore[misc]
        with self.assertRaises(TypeError):
            LkgPhase3AuthorityPair()
        with self.assertRaises(TypeError):
            LkgPhase3AuthorityPair._from_validated(
                authority=self.authority,
                verified_latest_reference=self.verified_latest,
                construction_token=object(),
            )

    def test_every_d2_persisted_d1_identity_field_is_compared(self) -> None:
        identity_fields = tuple(
            field.name
            for field in fields(PersistedLkgPhase3AuthorityReference)
            if field.name not in _D2_METADATA_FIELDS
        )
        digest_fields = {
            name
            for name in identity_fields
            if name.endswith("digest") or name.endswith("sha256")
        }
        integer_fields = {"evaluated_ef", "qualification_expected_query_count"}

        for field_name in identity_fields:
            if field_name in digest_fields:
                replacement: object = "f" * 64
            elif field_name in integer_fields:
                replacement = getattr(self.persisted, field_name) + 1
            elif field_name == "metric":
                replacement = Metric.COSINE.value
            else:
                replacement = f"different-{field_name}"
            changed = replace(self.persisted, **{field_name: replacement})
            with self.subTest(field=field_name), self.assertRaisesRegex(
                ValueError, field_name
            ):
                bind_lkg_phase3_authority(
                    authority=self.authority,
                    verified_latest_reference=_forge_verified_latest(changed),
                )

    def test_d2_record_metadata_is_not_compared_to_d1(self) -> None:
        with LkgPhase3AuthorityReferenceStore(
            self.root / "different-record-metadata.db"
        ) as store:
            store.append(
                _authority(42),
                persisted_at_utc="2026-08-08T16:01:00.000000Z",
            )
            changed = store.append(
                self.authority,
                persisted_at_utc="2026-08-08T16:02:00.000000Z",
            ).reference
            latest = store.load_verified_latest()
        assert latest is not None

        self.assertNotEqual(changed.sequence_number, self.persisted.sequence_number)
        self.assertNotEqual(changed.persisted_at_utc, self.persisted.persisted_at_utc)
        self.assertNotEqual(
            changed.previous_record_digest, self.persisted.previous_record_digest
        )
        self.assertNotEqual(
            changed.canonical_record_digest, self.persisted.canonical_record_digest
        )

        pair = bind_lkg_phase3_authority(
            authority=self.authority,
            verified_latest_reference=latest,
        )

        self.assertEqual(pair.verified_latest_reference.reference, changed)

    def test_d1_only_d2_only_and_plain_historical_reference_are_rejected(self) -> None:
        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(  # type: ignore[call-arg]
                authority=self.authority
            )
        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(  # type: ignore[call-arg]
                verified_latest_reference=self.verified_latest
            )
        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(
                authority=self.authority,
                verified_latest_reference=self.persisted,
            )

    def test_object_forged_or_nonconcrete_values_are_rejected(self) -> None:
        forged_authority = object.__new__(LkgPhase3Authority)
        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(
                authority=forged_authority,
                verified_latest_reference=self.verified_latest,
            )

        forged_latest = object.__new__(VerifiedLatestLkgPhase3AuthorityReference)
        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(
                authority=self.authority,
                verified_latest_reference=forged_latest,
            )

        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(
                authority=object(),
                verified_latest_reference=self.verified_latest,
            )
        with self.assertRaises(TypeError):
            bind_lkg_phase3_authority(
                authority=self.authority,
                verified_latest_reference=object(),
            )

    def test_neutral_module_has_only_d1_d2_contract_dependencies(self) -> None:
        source = Path(binding_module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        forbidden_fragments = (
            "canary_admission",
            "policy",
            "actuation",
            "milvus",
            "lkg_qualification",
            "lkg_phase2",
            "lkg_run_binding",
        )
        for fragment in forbidden_fragments:
            with self.subTest(fragment=fragment):
                self.assertFalse(any(fragment in name for name in imports), imports)
        self.assertEqual(
            binding_module.__all__,
            ["LkgPhase3AuthorityPair", "bind_lkg_phase3_authority"],
        )

    def test_admission_compatibility_names_are_aliases_not_duplicates(self) -> None:
        from vdbench.canary_admission import (
            Stage4LkgAuthorityPair,
            bind_stage4_lkg_authority,
        )

        self.assertIs(Stage4LkgAuthorityPair, LkgPhase3AuthorityPair)
        self.assertIs(bind_stage4_lkg_authority, bind_lkg_phase3_authority)


if __name__ == "__main__":
    unittest.main()
