"""Subprocess-free Docker container-health probe for live gRPC processes.

The Docker CLI forks a child process.  That is unsafe once gRPC owns active
polling file descriptors, so live query paths inspect the local Docker Unix
socket directly instead.  This module exposes only the existing lightweight
``StackHealth`` value type and never performs a Milvus query or mutation.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
from collections.abc import Callable, Mapping
from pathlib import Path
from urllib.parse import quote

from .milvus_actuation import StackHealth

__all__ = ["DockerSocketHealthProbe"]


DockerInspector = Callable[[str], object]


class DockerSocketHealthProbe:
    """Read etcd/MinIO health from Docker Engine without spawning a process."""

    def __init__(
        self,
        *,
        etcd_container: str,
        minio_container: str,
        socket_path: str | os.PathLike[str] | None = None,
        timeout_seconds: float = 2.0,
        inspector: DockerInspector | None = None,
    ) -> None:
        for name, value in (
            ("etcd_container", etcd_container),
            ("minio_container", minio_container),
        ):
            if not isinstance(value, str) or not value or "/" in value:
                raise ValueError(f"{name} must be a non-empty Docker container name")
        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or float(timeout_seconds) <= 0.0
        ):
            raise ValueError("timeout_seconds must be positive")
        self.etcd_container = etcd_container
        self.minio_container = minio_container
        self.timeout_seconds = float(timeout_seconds)
        self._socket_path = (
            Path(socket_path) if socket_path is not None else self._default_socket_path()
        )
        self._inspector = inspector or self._inspect_via_socket

    @staticmethod
    def _default_socket_path() -> Path:
        endpoint = os.environ.get("DOCKER_HOST", "")
        if endpoint.startswith("unix://"):
            return Path(endpoint.removeprefix("unix://"))
        candidates = (Path("/var/run/docker.sock"), Path.home() / ".docker" / "run" / "docker.sock")
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]

    def check(self) -> StackHealth:
        """Return false health rather than raising across the gRPC boundary."""

        etcd_ok, etcd_detail = self._container_health(self.etcd_container)
        minio_ok, minio_detail = self._container_health(self.minio_container)
        return StackHealth(
            etcd_healthy=etcd_ok,
            minio_healthy=minio_ok,
            detail=f"{etcd_detail}; {minio_detail}",
        )

    def _container_health(self, container: str) -> tuple[bool, str]:
        try:
            document = self._inspector(container)
            status = _health_status(document)
        except Exception:  # injected/external boundary is deliberately fail-closed  # noqa: BLE001
            return False, f"{container}=unavailable"
        return status == "healthy", f"{container}={status or 'unavailable'}"

    def _inspect_via_socket(self, container: str) -> object:
        request = (
            f"GET /containers/{quote(container, safe='')}/json HTTP/1.1\r\n"
            "Host: docker\r\n"
            "Connection: close\r\n"
            "Accept: application/json\r\n\r\n"
        ).encode("ascii")
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.settimeout(self.timeout_seconds)
            client.connect(os.fspath(self._socket_path))
            client.sendall(request)
            response = http.client.HTTPResponse(client)
            response.begin()
            payload = response.read()
            if response.status != 200:
                raise OSError(f"docker inspect status {response.status}")
        finally:
            client.close()
        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OSError("Docker Engine returned malformed JSON") from exc


def _health_status(value: object) -> str | None:
    if not isinstance(value, Mapping):
        raise ValueError("Docker inspect response is not a mapping")  # domain error type carries the governed reason code  # noqa: TRY004
    state = value.get("State")
    if not isinstance(state, Mapping):
        raise ValueError("Docker inspect response has no State")  # domain error type carries the governed reason code  # noqa: TRY004
    health = state.get("Health")
    if not isinstance(health, Mapping):
        return None
    status = health.get("Status")
    return status if isinstance(status, str) else None
