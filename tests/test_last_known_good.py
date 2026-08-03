import ast
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from vdbench.drift import DetectorState, DriftClassification, DriftDecision
from vdbench.last_known_good import (
    REASON_IDENTITY_MISMATCH,
    REASON_MALFORMED,
    REASON_MISSING,
    REASON_SCHEMA_MISMATCH,
    load_last_known_good,
    persist_last_known_good,
)
from vdbench.policy import (
    PolicyAction,
    PolicyMode,
    PreActionSafety,
    QualificationWindow,
    ResponseEstimate,
    evaluate_tuning_policy,
    qualify_last_known_good,
)

CONFIGURATION_ID = "config-v1"
INDEX_ID = "hnsw-m16-efc200-v1"
DATA_ID = "dataset-v1"
THRESHOLD_STRATUM = "target-025"
AUDIT_ID = "audit-lkg-001"
QUALIFIED_AT = "2026-08-03T12:34:56.123456Z"
REPOSITORY = Path(__file__).parents[1]
STORE_MODULE = REPOSITORY / "src" / "vdbench" / "last_known_good.py"


def qualification_window(sequence: int) -> QualificationWindow:
    return QualificationWindow(
        window_id=f"qualification-window-{sequence}",
        sequence_number=sequence,
        metric="L2",
        threshold_stratum=THRESHOLD_STRATUM,
        ef=400,
        mean_recall=0.97,
        recall_lower_bound_95=0.96,
        p95_latency_ms=4.0,
        latency_upper_bound_95_ms=4.5,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
    )


def qualification_windows() -> tuple[QualificationWindow, QualificationWindow]:
    return qualification_window(10), qualification_window(11)


def qualified_result():
    result = qualify_last_known_good(qualification_windows(), audit_id=AUDIT_ID)
    if not result.qualified:
        raise AssertionError(f"test fixture did not qualify: {result.reasons}")
    return result


def pre_action() -> PreActionSafety:
    return PreActionSafety(
        metric="L2",
        threshold_stratum=THRESHOLD_STRATUM,
        configuration_identity=CONFIGURATION_ID,
        index_identity=INDEX_ID,
        data_identity=DATA_ID,
        response_model_provenance="response-model-v1",
    )


def response_estimate(
    ef: int,
    *,
    mean_recall: float,
    recall_lower_bound: float,
    latency: float,
    latency_upper_bound: float,
) -> ResponseEstimate:
    return ResponseEstimate(
        metric="L2",
        threshold_stratum=THRESHOLD_STRATUM,
        ef=ef,
        mean_recall=mean_recall,
        recall_lower_bound_95=recall_lower_bound,
        p95_latency_ms=latency,
        latency_upper_bound_95_ms=latency_upper_bound,
        validated_model=True,
        provenance="response-model-v1",
    )


class LastKnownGoodPersistenceTests(unittest.TestCase):
    def test_persist_then_reload_matches_qualification_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "last-known-good.json"
            expected = qualified_result()

            persist_last_known_good(path, expected, qualified_at_utc=QUALIFIED_AT)
            actual = load_last_known_good(path, pre_action())

            self.assertEqual(actual, expected)
            self.assertEqual(
                actual.qualifying_window_ids,
                ("qualification-window-10", "qualification-window-11"),
            )
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(raw["schema_version"], 1)
            self.assertEqual(raw["qualified_at_utc"], QUALIFIED_AT)
            self.assertEqual(
                frozenset(raw),
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
                },
            )

    def test_corrupted_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "last-known-good.json"
            path.write_text('{"schema_version":1,"ef":', encoding="utf-8")

            actual = load_last_known_good(path, pre_action())

            self.assertFalse(actual.qualified)
            self.assertIsNone(actual.ef)
            self.assertEqual(actual.reasons, (REASON_MALFORMED,))

    def test_identity_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "last-known-good.json"
            persist_last_known_good(
                path, qualified_result(), qualified_at_utc=QUALIFIED_AT
            )

            for field in (
                "metric",
                "threshold_stratum",
                "configuration_identity",
                "index_identity",
                "data_identity",
            ):
                mismatch = {
                    "metric": "COSINE",
                    "threshold_stratum": "target-050",
                    "configuration_identity": "different-config",
                    "index_identity": "different-index",
                    "data_identity": "different-data",
                }[field]
                with self.subTest(field=field):
                    actual = load_last_known_good(
                        path, replace(pre_action(), **{field: mismatch})
                    )
                    self.assertFalse(actual.qualified)
                    self.assertIsNone(actual.ef)
                    self.assertEqual(actual.reasons, (REASON_IDENTITY_MISMATCH,))

    def test_missing_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "not-created.json"

            actual = load_last_known_good(path, pre_action())

            self.assertFalse(actual.qualified)
            self.assertIsNone(actual.ef)
            self.assertEqual(actual.reasons, (REASON_MISSING,))

    def test_schema_missing_or_unknown_fields_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "last-known-good.json"
            persist_last_known_good(
                path, qualified_result(), qualified_at_utc=QUALIFIED_AT
            )
            record = json.loads(path.read_text(encoding="utf-8"))
            cases = []
            missing = dict(record)
            missing.pop("data_identity")
            cases.append(missing)
            unknown = dict(record)
            unknown["unexpected"] = True
            cases.append(unknown)
            wrong_version = dict(record)
            wrong_version["schema_version"] = 2
            cases.append(wrong_version)

            for index, candidate in enumerate(cases):
                with self.subTest(index=index):
                    path.write_text(json.dumps(candidate), encoding="utf-8")
                    actual = load_last_known_good(path, pre_action())
                    self.assertFalse(actual.qualified)
                    self.assertIsNone(actual.ef)
                    self.assertEqual(actual.reasons, (REASON_SCHEMA_MISMATCH,))

    def test_separate_writer_and_reader_processes_preserve_exact_result(self) -> None:
        writer = """
import sys
from vdbench.last_known_good import persist_last_known_good
from vdbench.policy import QualificationWindow, qualify_last_known_good

def window(sequence):
    return QualificationWindow(
        window_id=f"qualification-window-{sequence}", sequence_number=sequence,
        metric="L2", threshold_stratum="target-025", ef=400,
        mean_recall=0.97, recall_lower_bound_95=0.96,
        p95_latency_ms=4.0, latency_upper_bound_95_ms=4.5,
        configuration_identity="config-v1",
        index_identity="hnsw-m16-efc200-v1", data_identity="dataset-v1",
    )

result = qualify_last_known_good((window(10), window(11)), audit_id="audit-lkg-001")
persist_last_known_good(sys.argv[1], result, qualified_at_utc="2026-08-03T12:34:56.123456Z")
"""
        reader = """
import json
import sys
from dataclasses import asdict
from vdbench.last_known_good import load_last_known_good
from vdbench.policy import PreActionSafety

pre_action = PreActionSafety(
    metric="L2", threshold_stratum="target-025",
    configuration_identity="config-v1", index_identity="hnsw-m16-efc200-v1",
    data_identity="dataset-v1", response_model_provenance="response-model-v1",
)
result = load_last_known_good(sys.argv[1], pre_action)
payload = asdict(result)
payload["metric"] = result.metric.value if result.metric else None
print(json.dumps(payload, sort_keys=True))
"""
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(REPOSITORY / "src")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "last-known-good.json"
            subprocess.run(
                [sys.executable, "-c", writer, str(path)],
                cwd=REPOSITORY,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            completed = subprocess.run(
                [sys.executable, "-c", reader, str(path)],
                cwd=REPOSITORY,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )

            actual = json.loads(completed.stdout)
            self.assertTrue(actual["qualified"])
            self.assertEqual(actual["ef"], 400)
            self.assertEqual(actual["metric"], "L2")
            self.assertEqual(
                actual["qualifying_window_ids"],
                ["qualification-window-10", "qualification-window-11"],
            )

    def test_loaded_result_is_accepted_as_policy_input_and_sources_are_exclusive(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "last-known-good.json"
            persist_last_known_good(
                path, qualified_result(), qualified_at_utc=QUALIFIED_AT
            )
            loaded = load_last_known_good(path, pre_action())
            estimates = {
                400: response_estimate(
                    400,
                    mean_recall=0.94,
                    recall_lower_bound=0.93,
                    latency=4.0,
                    latency_upper_bound=4.2,
                ),
                800: response_estimate(
                    800,
                    mean_recall=0.96,
                    recall_lower_bound=0.955,
                    latency=4.6,
                    latency_upper_bound=4.8,
                ),
            }
            drift = DriftDecision(
                state=DetectorState.DRIFT,
                classification=DriftClassification.QUALITY_DRIFT,
                significance_evidence_score=0.995,
                drift_magnitude=1.25,
            )
            keywords = {
                "current_ef": 400,
                "response_estimates": estimates,
                "pre_action": pre_action(),
                "canary_observation": None,
                "mode": PolicyMode.CANARY_ENABLED,
                "threshold_stratum": THRESHOLD_STRATUM,
                "audit_id": AUDIT_ID,
            }

            decision = evaluate_tuning_policy(
                drift,
                last_known_good=loaded,
                **keywords,
            )

            self.assertEqual(decision.action, PolicyAction.START_CANARY)
            self.assertEqual(decision.last_known_good_ef, 400)
            with self.assertRaisesRegex(ValueError, "mutually exclusive"):
                evaluate_tuning_policy(
                    drift,
                    qualification_windows=qualification_windows(),
                    last_known_good=loaded,
                    **keywords,
                )

    def test_store_module_has_no_pymilvus_import(self) -> None:
        tree = ast.parse(STORE_MODULE.read_text(encoding="utf-8"))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        self.assertFalse(any(name.startswith("pymilvus") for name in imports))


if __name__ == "__main__":
    unittest.main()
