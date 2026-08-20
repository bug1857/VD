from __future__ import annotations

import unittest
from dataclasses import replace

from vdbench.exp012_scale_contract import (
    EXP012_SCALE_CONTRACT_SCHEMA_VERSION,
    Exp012ScaleContractError,
    Exp012ScaleProfile,
    build_exp012_scale_contract,
    exp012_scale_contract_payload,
    verify_exp012_scale_contract,
)


class Exp012ScaleContractTests(unittest.TestCase):
    def test_exact_governed_profiles(self) -> None:
        cases = (
            (Exp012ScaleProfile.SCALE_2400, 2400, 12, 4800),
            (Exp012ScaleProfile.SCALE_10000, 10000, 50, 20000),
        )
        for profile, sources, windows, searches in cases:
            with self.subTest(profile=profile):
                contract = build_exp012_scale_contract(profile)
                self.assertEqual(contract.schema_version, EXP012_SCALE_CONTRACT_SCHEMA_VERSION)
                self.assertEqual(contract.target_source_records, sources)
                self.assertEqual(contract.window_query_count, 200)
                self.assertEqual(contract.expected_windows, windows)
                self.assertEqual(contract.expected_physical_searches, searches)
                self.assertEqual(verify_exp012_scale_contract(contract), contract)

    def test_profile_requires_exact_enum(self) -> None:
        for value in ("scale-2400", 2400, True):
            with self.subTest(value=value), self.assertRaises(Exp012ScaleContractError):
                build_exp012_scale_contract(value)  # type: ignore[arg-type]

    def test_every_derived_field_and_digest_is_recomputed(self) -> None:
        contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400)
        for name, value in (
            ("target_source_records", 10000),
            ("window_query_count", 201),
            ("expected_windows", 13),
            ("searches_per_source", 3),
            ("expected_physical_searches", 4799),
            ("contract_sha256", "0" * 64),
        ):
            with self.subTest(name=name), self.assertRaises(Exp012ScaleContractError):
                verify_exp012_scale_contract(replace(contract, **{name: value}))

    def test_bool_and_float_forgery_fail_type_exact(self) -> None:
        contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400)
        for value in (False, 2400.0):
            forged = object.__new__(type(contract))
            for name in contract.__dataclass_fields__:
                object.__setattr__(forged, name, getattr(contract, name))
            object.__setattr__(forged, "target_source_records", value)
            with self.assertRaises(Exp012ScaleContractError):
                verify_exp012_scale_contract(forged)

    def test_payload_is_detached_and_contains_no_exp010_schema(self) -> None:
        payload = exp012_scale_contract_payload(
            build_exp012_scale_contract(Exp012ScaleProfile.SCALE_10000)
        )
        self.assertNotIn("contract_sha256", payload)
        self.assertNotIn("exp010", repr(payload).lower())


if __name__ == "__main__":
    unittest.main()
