"""Offline contract tests for the EXP-008 H4 live-failure capture harness."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

from tests.test_exp008_acquisition import _configuration, _FakeServing, _FakeShadow


class Exp008FailureProbeTests(unittest.TestCase):
    @staticmethod
    def _capture(root: Path):
        from experiments.exp008_failure_probes import run_failure_probes
        from vdbench.exp008_acquisition import Exp008Runtime

        return run_failure_probes(
            configuration=_configuration(),
            runtime=Exp008Runtime(serving=_FakeServing(), shadow=_FakeShadow()),
            output_dir=root,
            pre_run_resources={"timestamp_utc": "2026-08-03T00:00:00Z"},
            capture_git={"commit": "fake-capture-commit", "dirty": False},
        )

    def test_fake_capture_exercises_all_registered_h4_failures_fail_closed(self) -> None:
        from experiments.exp008_failure_probes import run_failure_probes
        from vdbench.exp008_acquisition import Exp008Runtime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            result = run_failure_probes(
                configuration=_configuration(),
                runtime=Exp008Runtime(serving=_FakeServing(), shadow=_FakeShadow()),
                output_dir=root,
                pre_run_resources={"timestamp_utc": "2026-08-03T00:00:00Z"},
                capture_git={"commit": "fake-capture-commit", "dirty": False},
            )

            self.assertEqual(
                [probe.name for probe in result.probes],
                [
                    "foreground_recorder_failure",
                    "queue_full",
                    "publisher_unavailable",
                    "executor_timeout",
                    "identity_mismatch",
                    "worker_restart_partial_loss",
                ],
            )
            expected = {
                "foreground_recorder_failure": "RECORDER_FAILED",
                "queue_full": "PENDING_OBSERVATION_CAPACITY_EXCEEDED",
                "publisher_unavailable": "PUBLISH_OUTCOME_UNKNOWN",
                "executor_timeout": "EXECUTOR_CAPTURE_FAILED",
                "identity_mismatch": "TRACE_IDENTITY_MISMATCH",
                "worker_restart_partial_loss": "RESTART_LOSS_COUNT_EXACT",
            }
            for probe in result.probes:
                with self.subTest(probe=probe.name):
                    self.assertTrue(probe.foreground_success)
                    self.assertTrue(probe.fail_closed)
                    self.assertEqual(probe.expected_reason_code, expected[probe.name])
                    self.assertTrue((root / "probes" / f"{probe.name}.json").is_file())
            self.assertEqual(result.published_trace_count, 0)
            self.assertEqual(result.monitor_call_count, 0)
            self.assertEqual(result.policy_call_count, 0)
            self.assertEqual(result.actuation_call_count, 0)

    def test_capture_serializes_immutable_preflight_mapping(self) -> None:
        from experiments.exp008_failure_probes import run_failure_probes
        from vdbench.exp008_acquisition import Exp008Runtime

        class _MappingPreflightServing(_FakeServing):
            def preflight(self):  # type: ignore[no-untyped-def]
                return MappingProxyType(dict(super().preflight()))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            run_failure_probes(
                configuration=_configuration(),
                runtime=Exp008Runtime(
                    serving=_MappingPreflightServing(), shadow=_FakeShadow()
                ),
                output_dir=root,
                pre_run_resources={"timestamp_utc": "2026-08-03T00:00:00Z"},
                capture_git={"commit": "fake-capture-commit", "dirty": False},
            )
            for name in ("serving_preflight.json", "serving_postflight.json"):
                document = json.loads((root / name).read_text(encoding="utf-8"))
                self.assertEqual(
                    set(document),
                    {"exp008-l2-stationary", "exp008-cosine-stationary"},
                )
                self.assertNotIn("MonitorStreamKey(", "".join(document))

    def test_fresh_finalizer_writes_complete_manifest_only_for_valid_capture(self) -> None:
        from experiments.exp008_failure_probes import finalize_failure_probes

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            with patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ):
                result = finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )
            self.assertEqual(result.probe_count, 6)
            self.assertTrue(result.manifest_path.is_file())
            completion = json.loads(result.completion_path.read_text(encoding="utf-8"))
            self.assertEqual(completion["status"], "COMPLETE")
            self.assertEqual(completion["published_trace_count"], 0)
            self.assertEqual(completion["actuation_call_count"], 0)

    def test_fresh_finalizer_rejects_tampered_probe_evidence(self) -> None:
        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            path = root / "probes" / "identity_mismatch.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["fail_closed"] = False
            path.unlink()
            path.write_text(json.dumps(document), encoding="utf-8")
            with (
                patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ),
                self.assertRaisesRegex(EXP008FailureProbeError, "PROBE_EVIDENCE_INVALID"),
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )
            self.assertFalse((root / "completion.json").exists())

    def test_fresh_finalizer_rejects_tampered_observed_worker_reason(self) -> None:
        """The declared expected code cannot substitute for observed evidence."""

        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            path = root / "probes" / "executor_timeout.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["worker_cycle"]["reason_codes"] = []
            path.unlink()
            path.write_text(json.dumps(document), encoding="utf-8")
            with (
                patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ),
                self.assertRaisesRegex(EXP008FailureProbeError, "PROBE_EVIDENCE_INVALID"),
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )

    def test_fresh_finalizer_rejects_capture_receipt_disagreement(self) -> None:
        """The signed receipt must agree exactly with its persisted probe artifacts."""

        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            path = root / "capture_receipt.json"
            receipt = json.loads(path.read_text(encoding="utf-8"))
            receipt["probes"][0]["detail"] = "tampered receipt only"
            path.unlink()
            path.write_text(json.dumps(receipt), encoding="utf-8")
            with (
                patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ),
                self.assertRaisesRegex(EXP008FailureProbeError, "CAPTURE_RECEIPT_INVALID"),
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )

    def test_fresh_finalizer_rejects_unexpected_capture_artifact(self) -> None:
        """The immutable evidence inventory is closed, not merely hashed later."""

        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            (root / "unreviewed-extra.json").write_text("{}", encoding="utf-8")
            with (
                patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ),
                self.assertRaisesRegex(EXP008FailureProbeError, "CAPTURE_ARTIFACT_INVENTORY_INVALID"),
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )

    def test_fresh_finalizer_rejects_tampered_restart_state_evidence(self) -> None:
        """The restart-loss claim is checked against durable worker state."""

        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            path = root / "state" / "worker_restart_partial_loss" / "host-worker-state.json"
            state = json.loads(path.read_text(encoding="utf-8"))
            state["streams"][0]["restart_loss_count"] = 0
            path.unlink()
            path.write_text(json.dumps(state), encoding="utf-8")
            with (
                patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ),
                self.assertRaisesRegex(EXP008FailureProbeError, "STATE_EVIDENCE_INVALID"),
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )

    def test_independent_bundle_verifier_rejects_manifest_or_artifact_tampering(self) -> None:
        """Final evidence validates self hash, inventory, and receipt independently."""

        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
            verify_failure_probe_bundle,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            with patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "fake-capture-commit", "dirty": False},
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )
            verified = verify_failure_probe_bundle(root)
            self.assertEqual(verified["probe_count"], 6)

            manifest_path = root / "run_manifest.json"
            manifest_bytes = manifest_path.read_bytes()
            manifest = json.loads(manifest_bytes)
            manifest["self_sha256"] = "0" * 64
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(EXP008FailureProbeError, "MANIFEST_INVALID"):
                verify_failure_probe_bundle(root)
            manifest_path.write_bytes(manifest_bytes)

            path = root / "pre_run_resources.json"
            path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(EXP008FailureProbeError, "ARTIFACT_HASH_MISMATCH"):
                verify_failure_probe_bundle(root)

    def test_fresh_finalizer_rejects_changed_capture_commit(self) -> None:
        from experiments.exp008_failure_probes import (
            EXP008FailureProbeError,
            finalize_failure_probes,
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            self._capture(root)
            with patch(
                "experiments.exp008_failure_probes.git_state",
                return_value={"commit": "different-commit", "dirty": False},
            ), self.assertRaisesRegex(
                EXP008FailureProbeError,
                "CAPTURE_COMMIT_CHANGED_BEFORE_FINALIZATION",
            ):
                finalize_failure_probes(
                    output_dir=root,
                    post_run_resources={"timestamp_utc": "2026-08-03T00:01:00Z"},
                    repository=Path(directory),
                )
            self.assertFalse((root / "completion.json").exists())

    def test_finalize_only_cli_never_constructs_live_runtime(self) -> None:
        from experiments.exp008_failure_probes import FailureProbeRunResult, main

        result = FailureProbeRunResult(
            output_dir=Path("evidence"),
            manifest_path=Path("evidence/run_manifest.json"),
            completion_path=Path("evidence/completion.json"),
            probe_count=6,
        )
        with (
            patch(
                "experiments.exp008_failure_probes.capture_host_resource_snapshot",
                return_value={"timestamp_utc": "2026-08-03T00:01:00Z"},
            ),
            patch(
                "experiments.exp008_failure_probes.finalize_failure_probes",
                return_value=result,
            ) as finalize,
            patch(
                "experiments.exp008_failure_probes.build_live_runtime",
                side_effect=AssertionError("fresh finalizer must not open Milvus"),
            ),
        ):
            self.assertEqual(
                main(
                    [
                        "--dataset-dir", "ignored-dataset",
                        "--l2-baseline", "ignored-l2",
                        "--cosine-baseline", "ignored-cosine",
                        "--output-dir", "ignored-output",
                        "--finalize-only",
                    ]
                ),
                0,
            )
        finalize.assert_called_once()

    def test_capture_refuses_preexisting_evidence_root(self) -> None:
        from experiments.exp008_failure_probes import run_failure_probes
        from vdbench.exp008_acquisition import Exp008Runtime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "failure-probes"
            root.mkdir()
            with self.assertRaisesRegex(FileExistsError, "refusing to overwrite"):
                run_failure_probes(
                    configuration=_configuration(),
                    runtime=Exp008Runtime(serving=_FakeServing(), shadow=_FakeShadow()),
                    output_dir=root,
                    pre_run_resources={"timestamp_utc": "2026-08-03T00:00:00Z"},
                    capture_git={"commit": "fake-capture-commit", "dirty": False},
                )

    def test_harness_has_no_monitor_policy_or_safe_actuation_import(self) -> None:
        source = Path(__file__).parents[1] / "experiments" / "exp008_failure_probes.py"
        contents = source.read_text(encoding="utf-8")
        self.assertNotIn("workload_monitor", contents)
        self.assertNotIn("evaluate_drift_decision", contents)
        self.assertNotIn("evaluate_tuning_policy", contents)
        self.assertNotIn("SafeActuationBoundary", contents)


if __name__ == "__main__":
    unittest.main()
