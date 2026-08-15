"""FINDING-001: the v1 canonical serializer is frozen; v2 is strict.

Two properties are proven here.  First, `artifacts.canonical_json_bytes` still
produces the exact bytes every registered artifact and every V1-V4 campaign
digest was taken over -- including the real store-binding digests committed by
the live V4 campaign, pinned below as literals.  Second, the new
`canonical_serialization` v2 contract refuses the inputs v1 silently accepts.
"""

from __future__ import annotations

import hashlib
import json
import math
import unittest

from vdbench.artifacts import (
    CANONICAL_JSON_V1_SCHEMA_VERSION,
    canonical_json_bytes,
)
from vdbench.canonical_serialization import (
    CANONICAL_JSON_SCHEMA_VERSION,
    MAX_CANONICAL_DEPTH,
    CanonicalSerializationError,
    decode_strict_canonical_json,
    strict_canonical_digest,
    strict_canonical_json_bytes,
    validate_strict_canonical_value,
)

#: The exact stream document the live V4 campaign is bound to.
_V4_STREAM = {
    "configuration_identity": (
        "exp010-serving-config-v1:sha256:"
        "825931cd6efb30e070141f383d516a17bc581d996a563ef6be6f262ab8b366a9"
    ),
    "data_identity": (
        "DATASET-001-v1:sha256:"
        "b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9"
    ),
    "flat_binding_id": (
        "b63cf68a332127416d0cdf5372d4b8f4bac0c27d8f44b59c78b0953c4669bb46"
    ),
    "hnsw_binding_id": (
        "2db7944f6aa5190736ddafd1d25391aba648b5931734fc4b833ff02b3cec7bca"
    ),
    "metric": "L2",
    "stream_id": "exp010-live-l2-target075-v4",
    "threshold_stratum": "target-075",
}
_V4_ENVIRONMENT = (
    "49b309a4067cc89c7dadee0e54beb27a673851029672453b94351a6fdf9b6549"
)
_V4_REVISION = "764a91e39977773ba33ee6958f01c153c5453db2"

#: (domain, binding payload, digest actually stored by the V4 campaign).
_V4_STORE_BINDINGS = (
    (
        "v2_source",
        b"VD::HOST_RESPONSE_STORE_BINDING::V2\x00",
        {
            "consistency_level": "Strong",
            "environment_manifest_sha256": _V4_ENVIRONMENT,
            "schema_version": "response-profile-host-window-lineage-v2",
            "source_revision": _V4_REVISION,
            "stream": _V4_STREAM,
        },
        "66860da62e183052b7e7e4230358825ec2fb0c07638860ad573744832ad498c8",
    ),
    (
        "v2_shadow_attempts",
        b"VD::SHADOW_PHYSICAL_ATTEMPT_STORE::V1\x00",
        {
            "environment_manifest_sha256": _V4_ENVIRONMENT,
            "schema_version": "shadow-physical-attempt-store-v1",
            "source_revision": _V4_REVISION,
            "stream": _V4_STREAM,
        },
        "ae80f1faaa34052e8cfda331dd95423eb04c9ff1562249850cd8b052f8bcbf5a",
    ),
    (
        "v2_detector",
        b"VD::RESPONSE_PROFILE_DETECTOR_STORE::V2\x00",
        {
            "schema_version": "response-profile-detector-store-binding-v2",
            "stream": _V4_STREAM,
        },
        "0994d171fc79964267f4b4856ee2c11bd9365e8e1d2ddbe0e5c2a0a9e26da30c",
    ),
    (
        "v2_attestation",
        b"VD::REAL_DETECTOR_ATTESTATION_STORE::V1\x00",
        {
            "schema_version": "real-detector-attestation-v1",
            "stream": _V4_STREAM,
        },
        "3e235cede2853b70e74c348b7c3f736c7efb16cc049f1bf5be1cc86751c04c14",
    ),
    (
        "v2_window_finalization",
        b"VD::WINDOW_FINALIZATION_BINDING::V1\x00",
        {
            "environment_manifest_sha256": _V4_ENVIRONMENT,
            "schema_version": (
                "response-profile-window-finalization-binding-v1"
            ),
            "source_revision": _V4_REVISION,
            "stream": _V4_STREAM,
        },
        "a7417e47fb084e0e8fe7d8ad56b9c92708843b7120e5628fda11a44c7bfbaeba",
    ),
)

#: Golden v1 byte vectors.  Non-ASCII stays `\uXXXX`-escaped under v1.
_V1_GOLDEN = (
    ({}, b"{}\n"),
    ([], b"[]\n"),
    (None, b"null\n"),
    (True, b"true\n"),
    (0, b"0\n"),
    (-0.0, b"-0.0\n"),
    (1.5, b"1.5\n"),
    ("caf\u00e9", b'"caf\\u00e9"\n'),
    ({"b": 1, "a": 2}, b'{"a":2,"b":1}\n'),
    ({"k": [1, {"z": None, "y": "\u00fc"}]}, b'{"k":[1,{"y":"\\u00fc","z":null}]}\n'),
)


class CanonicalJsonV1FrozenTests(unittest.TestCase):
    """v1 is historical authority and may never change."""

    def test_schema_version_constant_is_stable(self) -> None:
        self.assertEqual(CANONICAL_JSON_V1_SCHEMA_VERSION, "vd-canonical-json-v1")

    def test_golden_byte_vectors_are_unchanged(self) -> None:
        for value, expected in _V1_GOLDEN:
            with self.subTest(value=value):
                self.assertEqual(canonical_json_bytes(value), expected)

    def test_matches_the_exact_expression_committed_before_hardening(self) -> None:
        """The pre-hardening one-liner, reproduced verbatim as the oracle."""

        def committed(value: object) -> bytes:
            return (
                json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("utf-8")

        corpus = [
            value for value, _ in _V1_GOLDEN
        ] + [
            _V4_STREAM,
            [payload for _, _, payload, _ in _V4_STORE_BINDINGS],
            {"deep": {"deeper": [1, 2, {"x": "\U0001f600"}]}},
            2**70,
            1e308,
            "\u65e5\u672c\u8a9e",
        ]
        for value in corpus:
            with self.subTest(value=repr(value)[:60]):
                self.assertEqual(canonical_json_bytes(value), committed(value))

    def test_reproduces_the_real_v4_campaign_store_binding_digests(self) -> None:
        """Byte-for-byte historical compatibility against committed evidence.

        These five digests are the `binding_sha256` values physically stored in
        the live V4 campaign's SQLite stores.  If the v1 serializer ever drifts,
        every V1-V4 digest becomes unverifiable and this fails first.
        """

        for name, domain, payload, expected in _V4_STORE_BINDINGS:
            with self.subTest(store=name):
                digest = hashlib.sha256(
                    domain + canonical_json_bytes(payload)
                ).hexdigest()
                self.assertEqual(digest, expected)

    def test_v1_retains_its_documented_non_finite_weakness(self) -> None:
        """Pinned so the known gap cannot be "fixed" without a version bump."""

        self.assertEqual(canonical_json_bytes(float("nan")), b"NaN\n")
        self.assertEqual(canonical_json_bytes(float("inf")), b"Infinity\n")


class StrictCanonicalJsonV2Tests(unittest.TestCase):
    def test_schema_version_constant_is_stable(self) -> None:
        self.assertEqual(CANONICAL_JSON_SCHEMA_VERSION, "vd-canonical-json-v2")

    def test_key_order_is_deterministic_regardless_of_insertion_order(self) -> None:
        first = strict_canonical_json_bytes({"b": 1, "a": 2, "c": 3})
        second = strict_canonical_json_bytes({"c": 3, "a": 2, "b": 1})
        self.assertEqual(first, second)
        self.assertEqual(first, b'{"a":2,"b":1,"c":3}\n')

    def test_utf8_is_emitted_without_ascii_escaping(self) -> None:
        self.assertEqual(
            strict_canonical_json_bytes({"k": "caf\u00e9"}),
            '{"k":"caf\u00e9"}\n'.encode("utf-8"),
        )

    def test_exactly_one_trailing_newline(self) -> None:
        encoded = strict_canonical_json_bytes({"a": 1})
        self.assertTrue(encoded.endswith(b"\n"))
        self.assertFalse(encoded.endswith(b"\n\n"))

    def test_non_finite_floats_are_refused(self) -> None:
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaises(CanonicalSerializationError) as caught:
                    strict_canonical_json_bytes({"k": value})
                self.assertEqual(caught.exception.code, "CANONICAL_JSON_NONFINITE")

    def test_non_string_keys_are_refused_rather_than_coerced(self) -> None:
        for key in (1, 1.5, True, None):
            with self.subTest(key=key):
                with self.assertRaises(CanonicalSerializationError) as caught:
                    strict_canonical_json_bytes({key: "v"})
                self.assertEqual(caught.exception.code, "CANONICAL_JSON_KEY_INVALID")

    def test_unsupported_types_are_refused(self) -> None:
        for value in (b"bytes", {1, 2}, object(), 1j):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises(CanonicalSerializationError) as caught:
                    strict_canonical_json_bytes({"k": value})
                self.assertEqual(caught.exception.code, "CANONICAL_JSON_TYPE_INVALID")

    def test_lone_surrogate_strings_are_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            strict_canonical_json_bytes({"k": "\ud800"})
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_STRING_INVALID")

    def test_non_nfc_strings_are_refused(self) -> None:
        decomposed = "cafe\u0301"
        self.assertNotEqual(decomposed, "caf\u00e9")
        with self.assertRaises(CanonicalSerializationError) as caught:
            strict_canonical_json_bytes({"k": decomposed})
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_STRING_INVALID")

    def test_excessive_nesting_fails_closed(self) -> None:
        value: object = "leaf"
        for _ in range(MAX_CANONICAL_DEPTH + 2):
            value = [value]
        with self.assertRaises(CanonicalSerializationError) as caught:
            strict_canonical_json_bytes(value)
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_DEPTH_EXCEEDED")

    def test_cyclic_documents_fail_closed(self) -> None:
        cycle: list[object] = []
        cycle.append(cycle)
        with self.assertRaises(CanonicalSerializationError):
            strict_canonical_json_bytes(cycle)

    def test_bool_is_not_confused_with_int(self) -> None:
        self.assertEqual(strict_canonical_json_bytes(True), b"true\n")
        self.assertEqual(strict_canonical_json_bytes(1), b"1\n")

    def test_digest_requires_an_explicit_non_empty_domain(self) -> None:
        for domain in (b"", "text", None):
            with self.subTest(domain=domain):
                with self.assertRaises(CanonicalSerializationError) as caught:
                    strict_canonical_digest(domain, {"a": 1})
                self.assertEqual(caught.exception.code, "CANONICAL_JSON_DOMAIN_INVALID")

    def test_identical_documents_under_distinct_domains_differ(self) -> None:
        payload = {"a": 1}
        self.assertNotEqual(
            strict_canonical_digest(b"VD::A\x00", payload),
            strict_canonical_digest(b"VD::B\x00", payload),
        )

    def test_digest_equals_sha256_of_domain_and_bytes(self) -> None:
        payload = {"a": 1, "b": ["x", 2.5]}
        self.assertEqual(
            strict_canonical_digest(b"VD::T\x00", payload),
            hashlib.sha256(
                b"VD::T\x00" + strict_canonical_json_bytes(payload)
            ).hexdigest(),
        )

    def test_v2_is_deliberately_not_byte_compatible_with_v1_for_non_ascii(
        self,
    ) -> None:
        """The versions are separate contracts, never interchangeable."""

        payload = {"k": "caf\u00e9"}
        self.assertNotEqual(
            canonical_json_bytes(payload), strict_canonical_json_bytes(payload)
        )

    def test_v2_matches_v1_for_pure_ascii_documents(self) -> None:
        for name, _domain, payload, _digest in _V4_STORE_BINDINGS:
            with self.subTest(store=name):
                self.assertEqual(
                    canonical_json_bytes(payload),
                    strict_canonical_json_bytes(payload),
                )


class StrictCanonicalDecodeTests(unittest.TestCase):
    def test_round_trip_of_canonical_bytes(self) -> None:
        payload = {"a": 1, "b": ["x", 2.5, None, True], "z": {"n": "\u00fc"}}
        encoded = strict_canonical_json_bytes(payload)
        self.assertEqual(decode_strict_canonical_json(encoded), payload)

    def test_duplicate_keys_are_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            decode_strict_canonical_json(b'{"a":1,"a":2}\n')
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_DUPLICATE_KEY")

    def test_non_canonical_key_order_is_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            decode_strict_canonical_json(b'{"b":1,"a":2}\n')
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_NOT_CANONICAL")

    def test_added_whitespace_is_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            decode_strict_canonical_json(b'{"a": 1}\n')
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_NOT_CANONICAL")

    def test_missing_trailing_newline_is_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            decode_strict_canonical_json(b'{"a":1}')
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_NOT_CANONICAL")

    def test_non_json_constants_are_refused(self) -> None:
        for blob in (b"NaN\n", b"Infinity\n", b"-Infinity\n"):
            with self.subTest(blob=blob):
                with self.assertRaises(CanonicalSerializationError) as caught:
                    decode_strict_canonical_json(blob)
                self.assertEqual(caught.exception.code, "CANONICAL_JSON_NONFINITE")

    def test_non_utf8_input_is_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            decode_strict_canonical_json(b"\xff\xfe\n")
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_INPUT_INVALID")

    def test_str_input_is_refused(self) -> None:
        with self.assertRaises(CanonicalSerializationError) as caught:
            decode_strict_canonical_json('{"a":1}\n')  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "CANONICAL_JSON_INPUT_INVALID")


class StrictValidatorTests(unittest.TestCase):
    def test_validator_accepts_every_permitted_shape(self) -> None:
        validate_strict_canonical_value(
            {
                "null": None,
                "bool": False,
                "int": -7,
                "big": 2**70,
                "float": 1.25,
                "str": "ok",
                "list": [1, "two", None],
                "tuple": (1, 2),
                "dict": {"nested": {"leaf": 0.5}},
            }
        )

    def test_validator_rejects_non_finite_leaf(self) -> None:
        with self.assertRaises(CanonicalSerializationError):
            validate_strict_canonical_value({"a": [1, [math.inf]]})


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
