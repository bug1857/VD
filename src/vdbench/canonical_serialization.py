"""Versioned strict canonical JSON for NEW governed schemas (FINDING-001).

The split-brain this closes:
    Two canonical-JSON spellings coexist in this repository. `artifacts.
    canonical_json_bytes` -- the v1 contract -- relies on `json.dumps`
    *defaults* for `ensure_ascii` and `allow_nan`, while a dozen call sites
    spell out `ensure_ascii=False, allow_nan=False` inline. The two disagree on
    non-ASCII escaping and, more seriously, on whether a non-finite float is an
    error or is silently emitted as the non-JSON tokens `NaN` / `Infinity`.
    Nothing in the source stated which spelling a new schema should adopt.

Why v1 is not "fixed" in place:
    Every V1-V4 campaign digest -- store bindings, source records, attempt
    events, detector events, attestations, finalization events -- is a SHA-256
    over `canonical_json_bytes` output. Changing that function would make all
    committed evidence unverifiable. v1 is therefore frozen exactly as it is
    and documented as historical authority; `tests/test_canonical_
    serialization.py` pins its bytes against golden vectors so it cannot drift.

What v2 adds:
    One explicit, self-describing contract for schemas introduced from here on:
    deterministic key order, UTF-8 without ASCII escaping, non-finite floats
    refused rather than encoded, exactly one trailing newline, an exact
    permitted value-type set, and a decoder that refuses duplicate keys and
    any input that would not re-encode to identical bytes.

Domain separation:
    Serialization is not identity. `strict_canonical_digest` requires an
    explicit domain prefix so two schemas that happen to canonicalize to the
    same bytes still never share a digest.

Authority:
    None. No policy, admission, grant, routing, activation, actuation, or
    candidate authority is created here, and nothing in this module contacts a
    service or reads campaign evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from typing import Any

__all__ = [
    "CANONICAL_JSON_SCHEMA_VERSION",
    "MAX_CANONICAL_DEPTH",
    "CanonicalSerializationError",
    "decode_strict_canonical_json",
    "strict_canonical_digest",
    "strict_canonical_json_bytes",
    "validate_strict_canonical_value",
]


CANONICAL_JSON_SCHEMA_VERSION = "vd-canonical-json-v2"

#: Bounded nesting fails closed on a cyclic or pathological document instead of
#: exhausting the interpreter stack inside `json.dumps`.
MAX_CANONICAL_DEPTH = 32


class CanonicalSerializationError(ValueError):
    """Fail-closed canonical-serialization error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> CanonicalSerializationError:
    return CanonicalSerializationError(code, message)


def _validate_text(value: str, *, code: str) -> None:
    # A lone surrogate survives `json.dumps` but cannot be encoded to UTF-8,
    # so it must be refused before it reaches the encoder.
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _error(code, "string is not UTF-8 encodable") from exc
    if unicodedata.normalize("NFC", value) != value:
        raise _error(code, "string is not NFC-normalized")


def validate_strict_canonical_value(value: object, *, _depth: int = 0) -> None:
    """Refuse anything the v2 contract does not define exactly.

    Deliberately stricter than `json.dumps`: `bool` is checked before `int`
    (it is a subclass), non-finite floats are refused rather than emitted as
    `NaN`/`Infinity`, and only `str` keys are permitted, so no int/float/bool
    key is ever coerced to a string behind the caller's back.
    """

    if _depth > MAX_CANONICAL_DEPTH:
        raise _error("CANONICAL_JSON_DEPTH_EXCEEDED")
    if value is None or type(value) is bool or type(value) is str:
        if type(value) is str:
            _validate_text(value, code="CANONICAL_JSON_STRING_INVALID")
        return
    if type(value) is int:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise _error(
                "CANONICAL_JSON_NONFINITE", "float values must be finite"
            )
        return
    if type(value) is list or type(value) is tuple:
        for item in value:
            validate_strict_canonical_value(item, _depth=_depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise _error(
                    "CANONICAL_JSON_KEY_INVALID", "object keys must be str"
                )
            _validate_text(key, code="CANONICAL_JSON_KEY_INVALID")
            validate_strict_canonical_value(item, _depth=_depth + 1)
        return
    raise _error("CANONICAL_JSON_TYPE_INVALID", type(value).__name__)


def strict_canonical_json_bytes(value: object) -> bytes:
    """Encode one v2 document to its single canonical byte sequence.

    Every option `json.dumps` would otherwise default is stated explicitly, so
    the contract is readable at the call site rather than inherited from the
    standard library's current defaults.
    """

    validate_strict_canonical_value(value)
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            default=None,
            check_circular=True,
        )
    except (TypeError, ValueError) as exc:  # defence in depth behind the walk
        raise _error("CANONICAL_JSON_ENCODE_FAILED", str(exc)) from exc
    return (encoded + "\n").encode("utf-8")


def strict_canonical_digest(domain: bytes, value: object) -> str:
    """Digest one v2 document under an explicit, mandatory domain prefix."""

    if type(domain) is not bytes or not domain:
        raise _error("CANONICAL_JSON_DOMAIN_INVALID")
    return hashlib.sha256(domain + strict_canonical_json_bytes(value)).hexdigest()


def _no_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _error("CANONICAL_JSON_DUPLICATE_KEY", key)
        result[key] = value
    return result


def decode_strict_canonical_json(data: bytes) -> object:
    """Decode v2 bytes, refusing anything that is not already canonical.

    Round-trip equality is the actual check: a document that decodes but would
    re-encode to different bytes (reordered keys, added whitespace, a repeated
    key, `NaN`) is rejected, so a decoded value is always the exact value whose
    digest was taken.
    """

    if type(data) is not bytes:
        raise _error("CANONICAL_JSON_INPUT_INVALID")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _error("CANONICAL_JSON_INPUT_INVALID", "input is not UTF-8") from exc
    try:
        value = json.loads(
            text, object_pairs_hook=_no_duplicate_pairs, parse_constant=_constant
        )
    except CanonicalSerializationError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise _error("CANONICAL_JSON_INPUT_INVALID", str(exc)) from exc
    if strict_canonical_json_bytes(value) != data:
        raise _error("CANONICAL_JSON_NOT_CANONICAL")
    return value


def _constant(name: str) -> object:
    # `NaN` / `Infinity` / `-Infinity` are not JSON and are never v2 documents.
    raise _error("CANONICAL_JSON_NONFINITE", name)
