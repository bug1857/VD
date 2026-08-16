"""Tests for the subprocess-free Docker health probe used by live gRPC paths."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path


class DockerSocketHealthProbeTests(unittest.TestCase):
    def test_both_healthy_containers_pass_without_a_cli_subprocess(self) -> None:
        from vdbench.docker_health import DockerSocketHealthProbe

        calls: list[str] = []

        def inspect(name: str) -> object:
            calls.append(name)
            return {"State": {"Health": {"Status": "healthy"}}}

        result = DockerSocketHealthProbe(
            etcd_container="milvus-etcd",
            minio_container="milvus-minio",
            inspector=inspect,
        ).check()

        self.assertTrue(result.etcd_healthy)
        self.assertTrue(result.minio_healthy)
        self.assertEqual(calls, ["milvus-etcd", "milvus-minio"])

    def test_unhealthy_or_unavailable_container_fails_closed(self) -> None:
        from vdbench.docker_health import DockerSocketHealthProbe

        def inspect(name: str) -> object:
            if name == "milvus-etcd":
                return {"State": {"Health": {"Status": "unhealthy"}}}
            raise OSError("socket unavailable")

        result = DockerSocketHealthProbe(
            etcd_container="milvus-etcd",
            minio_container="milvus-minio",
            inspector=inspect,
        ).check()

        self.assertFalse(result.etcd_healthy)
        self.assertFalse(result.minio_healthy)
        self.assertIn("milvus-etcd=unhealthy", result.detail)
        self.assertIn("milvus-minio=unavailable", result.detail)

    def test_live_probe_source_contains_no_subprocess_dependency(self) -> None:
        source = Path(__file__).parents[1] / "src/vdbench/docker_health.py"
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        self.assertNotIn("subprocess", imported)


if __name__ == "__main__":
    unittest.main()
