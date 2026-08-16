"""Real-application ingress for the EXP-010 v2 host (framework-free).

Purpose:
    Give an external application one narrow, production-quality boundary
    through which it supplies a genuine search request. `admit(payload)`
    validates the payload, builds exactly one `RangeQueryRequest` from
    server-pinned governed identities plus the caller's own request id and
    query vector, and calls `Exp010LiveRunner.serve(...)` exactly once.

Genuine workload:
    This module generates nothing. There is no vector sampler, no random
    source, no DATASET-002/003 or EXP-001 replay, no historical trace replay,
    and no benchmark generator. `query_vector` has exactly one origin: the
    external payload. A request is genuine solely because an application
    supplied it.

Authority:
    Every governed identity -- stream key, configuration identity, data
    identity, binding ids, environment digest, source revision, metric,
    stratum, radius, range filter, limit, served ef -- is pinned at
    construction by the operator. None may be supplied, overridden, or
    influenced by an HTTP caller. An unknown payload key is rejected outright.

Gate separation:
    The request path may call `serve(...)` only. It never calls
    `process_ready_windows`, `trigger_state`, or `capture_exp010_population`;
    Gates C/D/E stay operator-controlled.

Transport:
    Deliberately framework-free -- the project pins exactly three runtime
    dependencies (cryptography, numpy, pymilvus), so no web framework is
    available or added. `Exp010StdlibSearchHandler` is an optional thin
    `http.server` adapter over this same object for operators who want
    `POST /api/v1/search`; the core boundary has no HTTP dependency.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .exp010_live_runner import Exp010LiveRunner
from .exp010_serving_configuration import (
    Exp010ServingConfiguration,
    derive_serving_configuration_identity,
)
from .host_observation import RangeQueryRequest
from .host_window_lineage import HostResponseCommitError, V2VisibleResponse

__all__ = [
    "PAYLOAD_FIELDS",
    "Exp010IngressError",
    "Exp010RequestIngress",
    "Exp010StdlibSearchHandler",
]


#: The complete, closed set of keys an external caller may supply.
PAYLOAD_FIELDS = frozenset({"request_id", "query_vector"})

#: Anything an application might try to send that would establish authority.
_FORBIDDEN_PAYLOAD_FIELDS = frozenset(
    {
        "metric", "threshold_stratum", "threshold_radius", "radius",
        "range_filter", "limit", "served_ef", "stream_key", "stream_id",
        "configuration_identity", "data_identity", "flat_binding_id",
        "hnsw_binding_id", "environment_manifest_sha256", "source_revision",
        "detector_seed", "consistency_level", "dimensions",
    }
)


class Exp010IngressError(RuntimeError):
    """Fail-closed ingress error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> Exp010IngressError:
    return Exp010IngressError(code, message)


@dataclass(frozen=True, slots=True)
class _AdmissionResult:
    """One admitted request and the response that became visible for it."""

    request_id: int | str
    response: V2VisibleResponse


class Exp010RequestIngress:
    """The external-application boundary onto `Exp010LiveRunner.serve`."""

    def __init__(
        self,
        *,
        runner: Exp010LiveRunner,
        serving_configuration: Exp010ServingConfiguration,
    ) -> None:
        if type(runner) is not Exp010LiveRunner:
            raise _error("INGRESS_RUNNER_INVALID")
        if type(serving_configuration) is not Exp010ServingConfiguration:
            raise _error("INGRESS_CONFIGURATION_INVALID")
        self._runner = runner
        self.serving_configuration = serving_configuration
        self.configuration_identity = derive_serving_configuration_identity(
            serving_configuration
        )
        stream_key = runner.composition.stream_key
        # The derived serving-configuration identity must be the one the stream
        # is actually pinned to, or the ingress would admit requests under an
        # identity the detector head would later refuse.
        if stream_key.configuration_identity != self.configuration_identity:
            raise _error(
                "INGRESS_CONFIGURATION_IDENTITY_MISMATCH",
                "stream configuration_identity is not the derived serving identity",
            )
        if stream_key.metric is not serving_configuration.metric:
            raise _error("INGRESS_METRIC_MISMATCH")
        if stream_key.threshold_stratum != serving_configuration.threshold_stratum:
            raise _error("INGRESS_STRATUM_MISMATCH")
        self.stream_key = stream_key

    # -- admission -------------------------------------------------------

    def admit(self, payload: Mapping[str, Any]) -> _AdmissionResult:
        """Admit exactly one genuine external request.

        Validation happens entirely before `serve`, so a rejected payload
        creates no source membership. A duplicate request id is refused by the
        durable store (`HOST_SOURCE_QUERY_ID_DUPLICATE`) and is never remapped
        to a fresh id.
        """

        request_id, vector = self._validated_payload(payload)
        configuration = self.serving_configuration
        request = RangeQueryRequest(
            request_id,
            self.stream_key,
            vector,
            configuration.threshold_radius,
            configuration.range_filter,
            configuration.limit,
            configuration.served_ef,
        )
        # Exactly one serve call per admitted payload. No retry: a retried
        # request would be a second source member for one application request.
        try:
            response = self._runner.serve(request)
        except HostResponseCommitError as exc:
            if getattr(exc, "code", None) == "HOST_SOURCE_QUERY_ID_DUPLICATE":
                raise _error("INGRESS_REQUEST_ID_DUPLICATE", str(exc)) from exc
            # ADR-013: durable membership failed, so no visible success.
            raise _error("INGRESS_SOURCE_COMMIT_FAILED", str(exc)) from exc
        return _AdmissionResult(request_id=request.request_id, response=response)

    def _validated_payload(
        self, payload: Mapping[str, Any]
    ) -> tuple[int | str, tuple[float, ...]]:
        if not isinstance(payload, Mapping):
            raise _error("INGRESS_PAYLOAD_INVALID")
        keys = set(payload)
        intruders = keys & _FORBIDDEN_PAYLOAD_FIELDS
        if intruders:
            raise _error(
                "INGRESS_IDENTITY_OVERRIDE_REFUSED", ",".join(sorted(intruders))
            )
        if keys != PAYLOAD_FIELDS:
            raise _error("INGRESS_PAYLOAD_FIELDS_INVALID", ",".join(sorted(keys)))

        request_id = payload["request_id"]
        if isinstance(request_id, bool) or type(request_id) not in (int, str):
            raise _error("INGRESS_REQUEST_ID_INVALID")
        if isinstance(request_id, str) and not request_id.strip():
            raise _error("INGRESS_REQUEST_ID_INVALID")

        raw = payload["query_vector"]
        if not isinstance(raw, (list, tuple)):
            raise _error("INGRESS_QUERY_VECTOR_INVALID")
        if len(raw) != self.serving_configuration.dimensions:
            raise _error(
                "INGRESS_QUERY_VECTOR_DIMENSIONS_INVALID",
                f"expected {self.serving_configuration.dimensions} values",
            )
        values: list[float] = []
        for item in raw:
            if isinstance(item, bool) or not isinstance(item, (int, float)):
                raise _error("INGRESS_QUERY_VECTOR_INVALID")
            value = float(item)
            if not math.isfinite(value):
                raise _error("INGRESS_QUERY_VECTOR_NONFINITE")
            values.append(value)
        return request_id, tuple(values)


class Exp010StdlibSearchHandler:
    """Optional thin `http.server` adapter exposing POST /api/v1/search.

    Standard library only -- adding FastAPI/Flask/Uvicorn would breach the
    project's pinned three-dependency contract. Construct a handler class with
    `for_ingress(...)` and hand it to `http.server.HTTPServer`. This adapter
    performs no validation of its own: it decodes JSON and delegates to
    `Exp010RequestIngress.admit`, so the governed boundary stays in one place.
    """

    PATH = "/api/v1/search"

    @staticmethod
    def for_ingress(ingress: Exp010RequestIngress) -> type:
        from http.server import BaseHTTPRequestHandler

        if type(ingress) is not Exp010RequestIngress:
            raise _error("INGRESS_RUNNER_INVALID")

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != Exp010StdlibSearchHandler.PATH:
                    self._reply(404, {"error": "NOT_FOUND"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    body = json.loads(self.rfile.read(length).decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    self._reply(400, {"error": "INGRESS_PAYLOAD_INVALID"})
                    return
                try:
                    result = ingress.admit(body)
                except Exp010IngressError as exc:
                    status = 409 if exc.code == "INGRESS_REQUEST_ID_DUPLICATE" else 400
                    if exc.code == "INGRESS_SOURCE_COMMIT_FAILED":
                        status = 503
                    self._reply(status, {"error": exc.code})
                    return
                except Exception:  # serving failure: never fabricate a result  # noqa: BLE001
                    self._reply(503, {"error": "INGRESS_SERVING_FAILED"})
                    return
                outcome = result.response.served_outcome
                self._reply(200, {
                    "request_id": result.request_id,
                    "success": bool(outcome.success),
                    "result_count": int(outcome.result_count),
                    "source_sequence": result.response.committed_observation.source_sequence,
                })

            def _reply(self, status: int, document: dict) -> None:
                payload = json.dumps(document).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args: object) -> None:
                return

        return _Handler
