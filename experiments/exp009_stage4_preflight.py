"""Run one frozen, read-only ENV-001 Stage-4 runtime-preflight capture.

This is the only Stage-4 preflight entry point allowed to lazily construct a
PyMilvus client. It forces the pinned localhost URI and accepts no grant,
candidate-route, search, or configuration-mutation input. The injected core
records only health, collection-load, and index-identity facts.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
import json
from pathlib import Path
from typing import Protocol

from vdbench.config import ENV001_PINS
from vdbench.docker_health import DockerSocketHealthProbe
from vdbench.exp009_stage4_preflight import (
    PreflightEvidenceTarget,
    capture_read_only_preflight,
    target_from_artifacts,
    verify_preflight_evidence,
)


__all__ = ["PreflightInvocationError", "run_preflight"]


_BASELINE_RELATIVE = Path("artifacts/exp-005/baselines/l2-target-075-ef800-lkg400.json")
_DATASET_RELATIVE = Path("artifacts/exp-001/dataset")


class PreflightInvocationError(ValueError):
    """Fail-closed invocation error that never permits a candidate action."""


class ClientFactory(Protocol):
    def __call__(self, uri: str) -> object: ...


class HealthProbeFactory(Protocol):
    def __call__(self) -> object: ...


def run_preflight(
    *,
    output_dir: Path,
    target: PreflightEvidenceTarget,
    repository: Path,
    uri: str = ENV001_PINS.uri,
    client_factory: ClientFactory | None = None,
    health_probe_factory: HealthProbeFactory | None = None,
    utc_now: Callable[[], str] | None = None,
) -> dict[str, object]:
    """Run the one read-only preflight after validating every local boundary."""

    if uri != ENV001_PINS.uri:
        raise PreflightInvocationError("URI_NOT_PINNED")
    if not isinstance(target, PreflightEvidenceTarget):
        raise PreflightInvocationError("TARGET_INVALID")
    root = Path(output_dir)
    if root.exists() or root.is_symlink():
        raise PreflightInvocationError("OUTPUT_PATH_EXISTS")
    if not isinstance(repository, Path) or not repository.is_dir():
        raise PreflightInvocationError("REPOSITORY_INVALID")
    factory = client_factory or _default_client_factory
    health_factory = health_probe_factory or _default_health_probe_factory
    if not callable(factory) or not callable(health_factory):
        raise PreflightInvocationError("FACTORY_INVALID")
    client = factory(uri)
    health_probe = health_factory()
    capture = capture_read_only_preflight(
        target=target,
        output_dir=root,
        client=client,  # type: ignore[arg-type]
        stack_health_probe=health_probe,  # type: ignore[arg-type]
        repository=repository,
        utc_now=utc_now or _utc_now,
    )
    verified = verify_preflight_evidence(
        root,
        target=target,
        require_complete=False,
    )
    if not capture.complete or verified["status"] != "COMPLETE":
        raise PreflightInvocationError("PREFLIGHT_INCOMPLETE")
    return verify_preflight_evidence(root, target=target)


def _default_client_factory(uri: str) -> object:
    """Import PyMilvus only at the explicit live-invocation boundary."""

    from pymilvus import MilvusClient

    return MilvusClient(uri=uri)


def _default_health_probe_factory() -> DockerSocketHealthProbe:
    return DockerSocketHealthProbe(etcd_container="milvus-etcd", minio_container="milvus-minio")


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--uri", default=ENV001_PINS.uri)
    arguments = parser.parse_args()
    repository = Path(__file__).parents[1].resolve()
    target = target_from_artifacts(
        baseline_path=repository / _BASELINE_RELATIVE,
        dataset_dir=repository / _DATASET_RELATIVE,
    )
    try:
        result = run_preflight(
            output_dir=arguments.output_dir,
            target=target,
            repository=repository,
            uri=arguments.uri,
        )
    except PreflightInvocationError as error:
        root = Path(arguments.output_dir)
        document = root / "preflight_result.json"
        if document.is_file():
            print(document.read_text(encoding="utf-8").rstrip())
        raise SystemExit(str(error))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
