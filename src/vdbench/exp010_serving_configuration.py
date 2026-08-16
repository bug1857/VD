"""Governed v2 serving-configuration identity for the real EXP-010 host.

Purpose:
    Derive one canonical, self-identifying `configuration_identity` that binds
    exactly the *serving/query semantics* a v2 host uses to answer a genuine
    application request. It is the value carried in
    `MonitorStreamKey.configuration_identity`, which the detector copies into
    `EvidenceProvenance` and which the V2 head then requires to match.

Why a new domain rather than EXP-005's:
    `exp005_acquisition._derived_identities` binds `candidate_ef` and
    `last_known_good_ef` under schema `exp005-shadow-configuration-v1`. Those
    are candidate/canary concepts with no meaning in a v2 serving path, which
    serves exactly one `served_ef`. Reusing that schema would either fabricate
    candidate state or silently redefine an accepted schema, so this is a
    separate, explicitly versioned domain. EXP-005's identity is unchanged and
    historical evidence is never rewritten.

Domain separation (deliberate exclusions):
    This identity binds serving semantics ONLY. It must never absorb another
    authority domain, because collapsing domains would make one digest stand in
    for evidence it does not actually cover:
      * `data_identity` / dataset manifest digest -> DATASET-001 corpus domain
      * `flat_binding_id` / `hnsw_binding_id`      -> live index-binding domain
      * `environment_manifest_sha256`              -> environment domain
      * `deployment_identity`, `source_revision`, `stream_id`, `observed_at_utc`
                                                   -> environment/stream domains
      * `detector_seed`, `sentinel_ef`             -> detector-contract domain
        (`sentinel_ef` is already bound by
        `real_detector_attestation.detector_contract_identity` and by every
        `ShadowAuditTrace`, so it is governed there and is not duplicated here)
      * `candidate_ef` / `last_known_good_ef`      -> not part of v2 serving

Authority:
    An identity string only. No policy, admission, grant, routing, activation,
    actuation, or candidate authority is created here, and nothing in this
    module contacts a service.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .artifacts import canonical_json_bytes
from .config import (
    CONSISTENCY_LEVEL,
    RESULT_LIMIT,
    THRESHOLD_LABELS,
    Metric,
)
from .policy import ACTUATION_LADDER

__all__ = [
    "EXP010_SERVING_CONFIGURATION_PREFIX",
    "EXP010_SERVING_CONFIGURATION_SCHEMA_VERSION",
    "Exp010ServingConfiguration",
    "Exp010ServingConfigurationError",
    "derive_serving_configuration_identity",
    "serving_configuration_payload",
    "validate_governed_configuration_identity",
]


EXP010_SERVING_CONFIGURATION_SCHEMA_VERSION = "exp010-serving-configuration-v1"
EXP010_SERVING_CONFIGURATION_PREFIX = "exp010-serving-config-v1"
_CONFIGURATION_DOMAIN = b"VD::EXP010_SERVING_CONFIGURATION::V1\x00"

# The exact, closed field set. Any other key is a domain violation.
_CANONICAL_FIELDS = (
    "schema_version",
    "metric",
    "threshold_stratum",
    "threshold_radius",
    "range_filter",
    "limit",
    "served_ef",
    "dimensions",
    "consistency_level",
)

# Fields that belong to other authority domains and may never be folded in.
FORBIDDEN_FIELDS = frozenset(
    {
        "data_identity", "dataset_manifest_sha256", "generation_manifest_sha256",
        "flat_binding_id", "hnsw_binding_id", "flat_index_identity",
        "hnsw_index_identity", "environment_manifest_sha256",
        "deployment_identity", "source_revision", "stream_id", "detector_seed",
        "observed_at_utc", "sentinel_ef", "candidate_ef", "last_known_good_ef",
    }
)

_EXPECTED_DIMENSIONS = 128


class Exp010ServingConfigurationError(ValueError):
    """Fail-closed configuration error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010ServingConfigurationError:
    return Exp010ServingConfigurationError(code, message)


def _exact_int(value: object, *, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _error(code, "value must be an exact integer, not a bool")
    return value


def _finite_float(value: object, *, code: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _error(code, "value must be a real number")
    result = float(value)
    if not math.isfinite(result):
        raise _error(code, "value must be finite")
    return result


@dataclass(frozen=True, slots=True)
class Exp010ServingConfiguration:
    """The exact serving semantics one v2 host answers requests with."""

    metric: Metric
    threshold_stratum: str
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int
    dimensions: int
    consistency_level: str

    def __post_init__(self) -> None:
        if type(self.metric) is not Metric:
            raise _error("CONFIGURATION_METRIC_INVALID")
        if (
            not isinstance(self.threshold_stratum, str)
            or self.threshold_stratum not in THRESHOLD_LABELS
        ):
            raise _error("CONFIGURATION_STRATUM_INVALID")
        radius = _finite_float(self.threshold_radius, code="CONFIGURATION_RADIUS_INVALID")
        band = _finite_float(self.range_filter, code="CONFIGURATION_RANGE_FILTER_INVALID")
        object.__setattr__(self, "threshold_radius", radius)
        object.__setattr__(self, "range_filter", band)
        # Metric-specific range semantics, matching oracle.validate_range.
        if self.metric is Metric.L2:
            if band != 0.0 or not band < radius:
                raise _error("CONFIGURATION_RANGE_SEMANTICS_INVALID")
        else:
            if band != 1.0 or not -1.0 <= radius < band:
                raise _error("CONFIGURATION_RANGE_SEMANTICS_INVALID")
        limit = _exact_int(self.limit, code="CONFIGURATION_LIMIT_INVALID")
        if limit != RESULT_LIMIT:
            raise _error("CONFIGURATION_LIMIT_INVALID", f"limit must equal {RESULT_LIMIT}")
        served_ef = _exact_int(self.served_ef, code="CONFIGURATION_SERVED_EF_INVALID")
        if served_ef not in ACTUATION_LADDER:
            raise _error(
                "CONFIGURATION_SERVED_EF_INVALID",
                "served_ef must be in the governed actuation ladder",
            )
        dimensions = _exact_int(self.dimensions, code="CONFIGURATION_DIMENSIONS_INVALID")
        if dimensions != _EXPECTED_DIMENSIONS:
            raise _error("CONFIGURATION_DIMENSIONS_INVALID")
        if self.consistency_level != CONSISTENCY_LEVEL:
            raise _error("CONFIGURATION_CONSISTENCY_LEVEL_INVALID")


def serving_configuration_payload(
    configuration: Exp010ServingConfiguration,
) -> dict[str, object]:
    """Return the exact canonical payload the identity is derived from."""

    if type(configuration) is not Exp010ServingConfiguration:
        raise _error("CONFIGURATION_INVALID")
    return {
        "schema_version": EXP010_SERVING_CONFIGURATION_SCHEMA_VERSION,
        "metric": configuration.metric.value,
        "threshold_stratum": configuration.threshold_stratum,
        "threshold_radius": configuration.threshold_radius,
        "range_filter": configuration.range_filter,
        "limit": configuration.limit,
        "served_ef": configuration.served_ef,
        "dimensions": configuration.dimensions,
        "consistency_level": configuration.consistency_level,
    }


def derive_serving_configuration_identity(
    configuration: Exp010ServingConfiguration,
) -> str:
    """Derive `exp010-serving-config-v1:sha256:<64 hex>` from validated fields.

    Never accepts a caller-supplied digest. `canonical_json_bytes` sorts keys,
    so dictionary insertion order cannot affect the result.
    """

    payload = serving_configuration_payload(configuration)
    if tuple(sorted(payload)) != tuple(sorted(_CANONICAL_FIELDS)):
        raise _error("CONFIGURATION_FIELDS_INVALID")
    intruders = FORBIDDEN_FIELDS & set(payload)
    if intruders:
        raise _error(
            "CONFIGURATION_DOMAIN_VIOLATION", ",".join(sorted(intruders))
        )
    digest = hashlib.sha256(
        _CONFIGURATION_DOMAIN + canonical_json_bytes(payload)
    ).hexdigest()
    return f"{EXP010_SERVING_CONFIGURATION_PREFIX}:sha256:{digest}"


_GOVERNED_IDENTITY = re.compile(
    rf"{re.escape(EXP010_SERVING_CONFIGURATION_PREFIX)}:sha256:[0-9a-f]{{64}}\Z"
)


def validate_governed_configuration_identity(value: object) -> str:
    """Require the exact governed EXP-010 serving-identity syntax.

    NEW_OBSERVATION_A: `MonitorStreamKey` cannot itself demand this form,
    because it is also reconstructed from stored historical evidence and from
    other identity domains (EXP-005's, for one). This is therefore the
    new-record boundary check: an operator composing a *new* EXP-010 stream
    must supply a syntactically governed identity, not merely a non-empty
    string that would only be caught later by an exact binding comparison.

    Syntax only. It proves nothing about which configuration produced the
    digest; only `derive_serving_configuration_identity` can do that.
    """

    if not isinstance(value, str) or _GOVERNED_IDENTITY.fullmatch(value) is None:
        raise _error(
            "CONFIGURATION_IDENTITY_SYNTAX_INVALID",
            f"expected {EXP010_SERVING_CONFIGURATION_PREFIX}:sha256:<64 hex>",
        )
    return value


def serving_configuration_from_mapping(
    values: Mapping[str, Any],
) -> Exp010ServingConfiguration:
    """Build one configuration from an exact-keyed mapping, failing closed."""

    if not isinstance(values, Mapping):
        raise _error("CONFIGURATION_INVALID")
    expected = {
        "metric", "threshold_stratum", "threshold_radius", "range_filter",
        "limit", "served_ef", "dimensions", "consistency_level",
    }
    if set(values) != expected:
        raise _error("CONFIGURATION_FIELDS_INVALID")
    metric = values["metric"]
    if isinstance(metric, str):
        try:
            metric = Metric(metric)
        except ValueError as exc:
            raise _error("CONFIGURATION_METRIC_INVALID") from exc
    return Exp010ServingConfiguration(
        metric=metric,
        threshold_stratum=values["threshold_stratum"],
        threshold_radius=values["threshold_radius"],
        range_filter=values["range_filter"],
        limit=values["limit"],
        served_ef=values["served_ef"],
        dimensions=values["dimensions"],
        consistency_level=values["consistency_level"],
    )
