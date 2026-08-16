"""Offline atomic persistence for ADR-002 qualified last-known-good state.

Purpose:
    Persist one qualified policy result across process restarts and load it only
    when its strict schema and current pre-action identities still match.
Inputs:
    A qualified ``QualificationResult``, external RFC3339 UTC timestamp, path,
    and current ``PreActionSafety`` identity.
Outputs:
    The exact qualified result or a fail-closed unqualified result with one
    explicit reason code.
Dependencies:
    Python standard library plus policy/config value objects; never PyMilvus.
Failure modes:
    Missing, malformed, schema-mismatched, or identity-mismatched records are
    never trusted and never produce a fabricated ``ef``.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import THRESHOLD_LABELS, Metric
from .policy import (
    ACTUATION_LADDER,
    PreActionSafety,
    QualificationResult,
)

SCHEMA_VERSION = 1

REASON_MISSING = "LAST_KNOWN_GOOD_MISSING"
REASON_MALFORMED = "LAST_KNOWN_GOOD_MALFORMED"
REASON_SCHEMA_MISMATCH = "LAST_KNOWN_GOOD_SCHEMA_MISMATCH"
REASON_IDENTITY_MISMATCH = "LAST_KNOWN_GOOD_IDENTITY_MISMATCH"

_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "qualified",
        "ef",
        "metric",
        "threshold_stratum",
        "configuration_identity",
        "index_identity",
        "data_identity",
        "qualifying_window_ids",
        "qualified_at_utc",
    }
)
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class _MalformedRecord(ValueError):
    """Internal marker for JSON structures that cannot be trusted."""


def _unqualified(reason: str) -> QualificationResult:
    return QualificationResult(qualified=False, ef=None, reasons=(reason,))


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_rfc3339_utc(value: object) -> bool:
    if not isinstance(value, str) or _RFC3339_UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.utcoffset() is not None and parsed.utcoffset().total_seconds() == 0


def _validate_qualified_result(result: QualificationResult) -> None:
    if not isinstance(result, QualificationResult):
        raise TypeError("result must be a QualificationResult")
    if result.qualified is not True or result.reasons != ():
        raise ValueError("only a successfully qualified result may be persisted")
    if (
        isinstance(result.ef, bool)
        or not isinstance(result.ef, int)
        or result.ef not in ACTUATION_LADDER
    ):
        raise ValueError("qualified ef must be in the ADR-002 actuation ladder")
    if not isinstance(result.metric, Metric):
        raise ValueError("qualified metric must be a canonical Metric")  # domain error type carries the governed reason code  # noqa: TRY004
    if result.threshold_stratum not in THRESHOLD_LABELS:
        raise ValueError("qualified threshold stratum must be canonical")
    if not all(
        _nonempty(value)
        for value in (
            result.configuration_identity,
            result.index_identity,
            result.data_identity,
        )
    ):
        raise ValueError("qualified identity fields must be non-empty")
    if (
        not isinstance(result.qualifying_window_ids, tuple)
        or len(result.qualifying_window_ids) != 2
        or not all(_nonempty(value) for value in result.qualifying_window_ids)
        or result.qualifying_window_ids[0] == result.qualifying_window_ids[1]
    ):
        raise ValueError("exactly two distinct qualifying window IDs are required")


def _record_for(
    result: QualificationResult,
    *,
    qualified_at_utc: str,
) -> dict[str, object]:
    _validate_qualified_result(result)
    if not _valid_rfc3339_utc(qualified_at_utc):
        raise ValueError("qualified_at_utc must be an RFC3339 UTC timestamp ending Z")
    return {
        "schema_version": SCHEMA_VERSION,
        "qualified": True,
        "ef": result.ef,
        "metric": result.metric.value,
        "threshold_stratum": result.threshold_stratum,
        "configuration_identity": result.configuration_identity,
        "index_identity": result.index_identity,
        "data_identity": result.data_identity,
        "qualifying_window_ids": list(result.qualifying_window_ids),
        "qualified_at_utc": qualified_at_utc,
    }


def persist_last_known_good(
    path: str | os.PathLike[str],
    result: QualificationResult,
    *,
    qualified_at_utc: str,
) -> None:
    """Atomically persist one qualified result with restart-durable fsyncs."""

    target = Path(path)
    parent = target.parent
    if not parent.is_dir():
        raise FileNotFoundError(f"parent directory does not exist: {parent}")
    payload = json.dumps(
        _record_for(result, qualified_at_utc=qualified_at_utc),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=parent,
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = -1
            handle.write(payload)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory_descriptor = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _object_without_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _MalformedRecord(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _decode_record(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, REASON_MISSING
    except (OSError, UnicodeError):
        return None, REASON_MALFORMED
    try:
        decoded = json.loads(payload, object_pairs_hook=_object_without_duplicate_keys)
    except (json.JSONDecodeError, _MalformedRecord, TypeError, ValueError):
        return None, REASON_MALFORMED
    if not isinstance(decoded, dict):
        return None, REASON_MALFORMED
    if frozenset(decoded) != _RECORD_FIELDS:
        return None, REASON_SCHEMA_MISMATCH
    if type(decoded["schema_version"]) is not int or decoded["schema_version"] != 1:
        return None, REASON_SCHEMA_MISMATCH
    return decoded, None


def _qualification_from_record(
    record: dict[str, Any],
) -> QualificationResult | None:
    if record["qualified"] is not True:
        return None
    ef = record["ef"]
    if isinstance(ef, bool) or not isinstance(ef, int) or ef not in ACTUATION_LADDER:
        return None
    try:
        metric = Metric(record["metric"])
    except (TypeError, ValueError):
        return None
    threshold_stratum = record["threshold_stratum"]
    if threshold_stratum not in THRESHOLD_LABELS:
        return None
    identity_values = (
        record["configuration_identity"],
        record["index_identity"],
        record["data_identity"],
    )
    if not all(_nonempty(value) for value in identity_values):
        return None
    window_ids = record["qualifying_window_ids"]
    if (
        not isinstance(window_ids, list)
        or len(window_ids) != 2
        or not all(_nonempty(value) for value in window_ids)
        or window_ids[0] == window_ids[1]
    ):
        return None
    if not _valid_rfc3339_utc(record["qualified_at_utc"]):
        return None
    return QualificationResult(
        qualified=True,
        ef=ef,
        reasons=(),
        metric=metric,
        threshold_stratum=threshold_stratum,
        configuration_identity=identity_values[0],
        index_identity=identity_values[1],
        data_identity=identity_values[2],
        qualifying_window_ids=(window_ids[0], window_ids[1]),
    )


def _pre_action_identity_matches(
    qualification: QualificationResult,
    pre_action: PreActionSafety,
) -> bool:
    if not isinstance(pre_action, PreActionSafety):
        return False
    try:
        metric = Metric(pre_action.metric)
    except (TypeError, ValueError):
        return False
    if pre_action.threshold_stratum not in THRESHOLD_LABELS:
        return False
    if not all(
        _nonempty(value)
        for value in (
            pre_action.configuration_identity,
            pre_action.index_identity,
            pre_action.data_identity,
        )
    ):
        return False
    return bool(
        qualification.metric is metric
        and qualification.threshold_stratum == pre_action.threshold_stratum
        and qualification.configuration_identity == pre_action.configuration_identity
        and qualification.index_identity == pre_action.index_identity
        and qualification.data_identity == pre_action.data_identity
    )


def load_last_known_good(
    path: str | os.PathLike[str],
    pre_action: PreActionSafety,
) -> QualificationResult:
    """Load a strict record and fail closed unless current identities match."""

    record, failure = _decode_record(Path(path))
    if failure is not None or record is None:
        return _unqualified(failure or REASON_MALFORMED)
    qualification = _qualification_from_record(record)
    if qualification is None:
        return _unqualified(REASON_MALFORMED)
    if not _pre_action_identity_matches(qualification, pre_action):
        return _unqualified(REASON_IDENTITY_MISMATCH)
    return qualification


__all__ = [
    "REASON_IDENTITY_MISMATCH",
    "REASON_MALFORMED",
    "REASON_MISSING",
    "REASON_SCHEMA_MISMATCH",
    "SCHEMA_VERSION",
    "load_last_known_good",
    "persist_last_known_good",
]
