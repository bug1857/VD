from __future__ import annotations

import ast
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from vdbench.config import Metric
from vdbench.exp012_scale_contract import Exp012ScaleProfile, build_exp012_scale_contract
from vdbench.host_window_lineage import CommittedHostObservation
from vdbench.shadow_event_types import MonitorStreamKey
from vdbench import shadow_search_telemetry as telemetry_module
from vdbench.shadow_search_telemetry import (
    SQLiteShadowSearchTelemetryStore,
    ShadowSearchOutcome,
    ShadowSearchRole,
    ShadowSearchTelemetryBinding,
    ShadowSearchTelemetryError,
)


def _binding(profile=Exp012ScaleProfile.SCALE_2400):
    return ShadowSearchTelemetryBinding(
        campaign_id="exp012-scale-test",
        scale_contract=build_exp012_scale_contract(profile),
        stream_key=MonitorStreamKey(
            "stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw"
        ),
        source_revision="revision",
        environment_manifest_sha256="a" * 64,
    )


def _forged_stream(**overrides):
    values = {
        "stream_id": "stream",
        "metric": Metric.L2,
        "threshold_stratum": "target-075",
        "configuration_identity": "cfg",
        "data_identity": "data",
        "flat_binding_id": "flat",
        "hnsw_binding_id": "hnsw",
    }
    values.update(overrides)
    stream = object.__new__(MonitorStreamKey)
    for name, value in values.items():
        object.__setattr__(stream, name, value)
    return stream


def _binding_with_stream(stream):
    return ShadowSearchTelemetryBinding(
        campaign_id="exp012-scale-test",
        scale_contract=build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400),
        stream_key=stream,
        source_revision="revision",
        environment_manifest_sha256="a" * 64,
    )


def _append(store, *, source=0, role=ShadowSearchRole.FLAT_REFERENCE, start=10, end=20,
            outcome=ShadowSearchOutcome.SUCCEEDED):
    return store.append(
        window_sequence=source // 200,
        trace_sequence_index=(source % 200) // 50,
        attempt_sha256=f"{source // 50 + 1:064x}"[-64:],
        source_sequence=source,
        source_sha256=f"{source + 100:064x}"[-64:],
        query_id_sha256=f"{source + 200:064x}"[-64:],
        role=role,
        started_monotonic_ns=start,
        completed_monotonic_ns=end,
        outcome=outcome,
        error_classification=None if outcome is ShadowSearchOutcome.SUCCEEDED else "TimeoutError",
        result_count=3 if outcome is ShadowSearchOutcome.SUCCEEDED else None,
    )


class ShadowSearchTelemetryTests(unittest.TestCase):
    def test_forked_close_never_unlocks_parent_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteShadowSearchTelemetryStore(
                Path(directory) / "telemetry.sqlite3", binding=_binding()
            )
            with mock.patch(
                "vdbench.shadow_search_telemetry.os.getpid",
                return_value=store._pid + 1,
            ), mock.patch(
                "vdbench.shadow_search_telemetry.fcntl.flock"
            ) as flock:
                store.close()
            flock.assert_not_called()

    def test_stream_key_is_reconstructed_and_valid_value_round_trips(self) -> None:
        stream = MonitorStreamKey(
            "stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw"
        )
        binding = _binding_with_stream(stream)
        payload = telemetry_module._binding_payload(binding)
        self.assertEqual(binding.stream_key, stream)
        self.assertEqual(
            payload["stream"],
            {
                "stream_id": "stream",
                "metric": "L2",
                "threshold_stratum": "target-075",
                "configuration_identity": "cfg",
                "data_identity": "data",
                "flat_binding_id": "flat",
                "hnsw_binding_id": "hnsw",
            },
        )

    def test_forged_stream_keys_fail_constructor_reconstruction(self) -> None:
        for label, stream in (
            ("non_metric", _forged_stream(metric="L2")),
            ("empty", _forged_stream(data_identity="")),
            ("whitespace", _forged_stream(flat_binding_id=" flat")),
            ("control", _forged_stream(stream_id="stream\n")),
            ("non_nfc", _forged_stream(hnsw_binding_id="e\u0301")),
        ):
            with self.subTest(label=label), self.assertRaises(
                ShadowSearchTelemetryError
            ) as raised:
                _binding_with_stream(stream)
            self.assertEqual(raised.exception.code, "TELEMETRY_STREAM_INVALID")

    def test_forged_binding_cannot_bypass_stream_reconstruction(self) -> None:
        valid = _binding()
        forged = object.__new__(ShadowSearchTelemetryBinding)
        for name in (
            "campaign_id",
            "scale_contract",
            "source_revision",
            "environment_manifest_sha256",
        ):
            object.__setattr__(forged, name, getattr(valid, name))
        object.__setattr__(forged, "stream_key", _forged_stream(data_identity=""))
        with self.assertRaises(ShadowSearchTelemetryError) as raised:
            telemetry_module._binding_payload(forged)
        self.assertEqual(raised.exception.code, "TELEMETRY_STREAM_INVALID")

    def test_ledger_has_no_service_or_candidate_authority_dependency(self) -> None:
        module = (
            Path(__file__).parents[1]
            / "src"
            / "vdbench"
            / "shadow_search_telemetry.py"
        )
        tree = ast.parse(module.read_text(encoding="utf-8"))
        imported = {
            (node.module or "").lstrip(".").split(".")[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        } | {
            alias.name.split(".")[-1]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        self.assertFalse(
            imported
            & {
                "pymilvus", "milvus", "policy", "actuation",
                "canary_admission", "canary_activation", "canary_grant_store",
                "canary_routing",
            }
        )

    def test_append_restart_and_latency_derivation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "telemetry.sqlite3"
            with SQLiteShadowSearchTelemetryStore(path, binding=_binding()) as store:
                flat = _append(store)
                hnsw = _append(store, role=ShadowSearchRole.HNSW_SENTINEL, start=20, end=35)
                self.assertEqual((flat.latency_ns, flat.latency_ms), (10, 0.00001))
                self.assertEqual(hnsw.previous_record_sha256, flat.record_sha256)
                expected = store.records()
            with SQLiteShadowSearchTelemetryStore(path, binding=_binding()) as reopened:
                self.assertEqual(reopened.records(), expected)
                self.assertEqual(reopened.summary().record_count, 2)
                self.assertFalse(reopened.summary().complete)

    def test_duplicate_source_role_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            with SQLiteShadowSearchTelemetryStore(
                Path(raw) / "telemetry.sqlite3", binding=_binding()
            ) as store:
                _append(store)
                with self.assertRaises(ShadowSearchTelemetryError) as raised:
                    _append(store)
                self.assertEqual(raised.exception.code, "TELEMETRY_POSITION_DUPLICATE")

    def test_invalid_position_timing_and_scalar_types_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            with SQLiteShadowSearchTelemetryStore(
                Path(raw) / "telemetry.sqlite3", binding=_binding()
            ) as store:
                for kwargs in (
                    {"source": 200},
                    {"start": 21, "end": 20},
                    {"start": False, "end": 20},
                ):
                    with self.subTest(kwargs=kwargs), self.assertRaises(ShadowSearchTelemetryError):
                        if kwargs.get("source") == 200:
                            store.append(
                                window_sequence=0, trace_sequence_index=0,
                                attempt_sha256="1" * 64, source_sequence=200,
                                source_sha256="2" * 64, query_id_sha256="3" * 64,
                                role=ShadowSearchRole.FLAT_REFERENCE,
                                started_monotonic_ns=1, completed_monotonic_ns=2,
                                outcome=ShadowSearchOutcome.SUCCEEDED,
                                error_classification=None, result_count=1,
                            )
                        else:
                            _append(store, **kwargs)

    def test_failed_search_is_preserved_but_never_complete(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            with SQLiteShadowSearchTelemetryStore(
                Path(raw) / "telemetry.sqlite3", binding=_binding()
            ) as store:
                record = _append(store, outcome=ShadowSearchOutcome.FAILED)
                self.assertEqual(record.error_classification, "TimeoutError")
                summary = store.summary()
                self.assertEqual((summary.succeeded_count, summary.failed_count), (0, 1))
                self.assertFalse(summary.complete)

    def test_update_delete_and_canonical_tamper_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "telemetry.sqlite3"
            with SQLiteShadowSearchTelemetryStore(path, binding=_binding()) as store:
                _append(store)
                with self.assertRaises(sqlite3.DatabaseError):
                    store._connection.execute("UPDATE telemetry_records SET role='HNSW_SENTINEL'")
                with self.assertRaises(sqlite3.DatabaseError):
                    store._connection.execute("DELETE FROM telemetry_records")
            db = sqlite3.connect(path)
            trigger = db.execute(
                "SELECT sql FROM sqlite_schema WHERE name='telemetry_records_no_update'"
            ).fetchone()[0]
            db.execute("DROP TRIGGER telemetry_records_no_update")
            db.execute("UPDATE telemetry_records SET record_json=?", (b"{}",))
            db.execute(trigger)
            db.commit()
            db.close()
            with self.assertRaises(ShadowSearchTelemetryError):
                SQLiteShadowSearchTelemetryStore(path, binding=_binding())

    def test_binding_and_schema_substitution_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "telemetry.sqlite3"
            with SQLiteShadowSearchTelemetryStore(path, binding=_binding()):
                pass
            with self.assertRaises(ShadowSearchTelemetryError) as mismatch:
                SQLiteShadowSearchTelemetryStore(
                    path, binding=_binding(Exp012ScaleProfile.SCALE_10000)
                )
            self.assertEqual(mismatch.exception.code, "TELEMETRY_BINDING_MISMATCH")
            db = sqlite3.connect(path)
            db.execute("PRAGMA user_version=99")
            db.commit()
            db.close()
            with self.assertRaises(ShadowSearchTelemetryError) as schema:
                SQLiteShadowSearchTelemetryStore(path, binding=_binding())
            self.assertEqual(schema.exception.code, "TELEMETRY_SCHEMA_INVALID")

    def test_large_fake_histories_require_exact_cardinality_and_positions(self) -> None:
        for profile in Exp012ScaleProfile:
            contract = build_exp012_scale_contract(profile)
            started = time.perf_counter()
            records = tuple(
                SimpleNamespace(
                    source_sequence=source,
                    role=role,
                    outcome=ShadowSearchOutcome.SUCCEEDED,
                )
                for source in range(contract.target_source_records)
                for role in ShadowSearchRole
            )
            self.assertTrue(
                SQLiteShadowSearchTelemetryStore._is_complete(records, contract)
            )
            self.assertFalse(
                SQLiteShadowSearchTelemetryStore._is_complete(records[:-1], contract)
            )
            substituted = list(records)
            substituted[-1] = SimpleNamespace(
                source_sequence=0,
                role=ShadowSearchRole.FLAT_REFERENCE,
                outcome=ShadowSearchOutcome.SUCCEEDED,
            )
            self.assertFalse(
                SQLiteShadowSearchTelemetryStore._is_complete(tuple(substituted), contract)
            )
            self.assertLess(time.perf_counter() - started, 2.0)

    def test_completion_cross_checks_source_and_attempt_identities(self) -> None:
        contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400)
        sources = []
        records = []
        for source_sequence in range(contract.target_source_records):
            source = object.__new__(CommittedHostObservation)
            object.__setattr__(source, "source_sequence", source_sequence)
            object.__setattr__(source, "window_sequence", source_sequence // 200)
            object.__setattr__(source, "within_window_index", source_sequence % 200)
            object.__setattr__(source, "source_sha256", f"{source_sequence + 1:064x}"[-64:])
            object.__setattr__(source, "query_id_sha256", f"{source_sequence + 5000:064x}"[-64:])
            sources.append(source)
            attempt = f"{source_sequence // 50 + 9000:064x}"[-64:]
            for role in ShadowSearchRole:
                records.append(
                    SimpleNamespace(
                        source_sequence=source_sequence,
                        source_sha256=source.source_sha256,
                        query_id_sha256=source.query_id_sha256,
                        window_sequence=source.window_sequence,
                        trace_sequence_index=(source_sequence % 200) // 50,
                        attempt_sha256=attempt,
                        role=role,
                        outcome=ShadowSearchOutcome.SUCCEEDED,
                        record_sha256="f" * 64,
                    )
                )
        with tempfile.TemporaryDirectory() as raw, SQLiteShadowSearchTelemetryStore(
            Path(raw) / "telemetry.sqlite3", binding=_binding()
        ) as store, mock.patch(
            "vdbench.shadow_search_telemetry.build_shadow_attempt_identity",
            side_effect=lambda group, *, trace_sequence_index: SimpleNamespace(
                attempt_sha256=f"{group[0].source_sequence // 50 + 9000:064x}"[-64:]
            ),
        ):
            store.records = lambda: tuple(records)
            summary = store.verify_completion(tuple(sources))
            self.assertTrue(summary.complete)
            records[-1].source_sha256 = "0" * 64
            with self.assertRaises(ShadowSearchTelemetryError) as raised:
                store.verify_completion(tuple(sources))
            self.assertEqual(
                raised.exception.code, "TELEMETRY_SOURCE_BINDING_MISMATCH"
            )

    def test_checkpoint_prefix_requires_exact_two_roles_for_canonical_window(self) -> None:
        sources = []
        records = []
        for source_sequence in range(200):
            source = object.__new__(CommittedHostObservation)
            object.__setattr__(source, "source_sequence", source_sequence)
            object.__setattr__(source, "window_sequence", 0)
            object.__setattr__(source, "within_window_index", source_sequence)
            object.__setattr__(source, "source_sha256", f"{source_sequence + 1:064x}"[-64:])
            object.__setattr__(source, "query_id_sha256", f"{source_sequence + 5000:064x}"[-64:])
            sources.append(source)
            for role in ShadowSearchRole:
                records.append(
                    SimpleNamespace(
                        source_sequence=source_sequence,
                        source_sha256=source.source_sha256,
                        query_id_sha256=source.query_id_sha256,
                        window_sequence=0,
                        trace_sequence_index=source_sequence // 50,
                        attempt_sha256=f"{source_sequence // 50 + 9000:064x}"[-64:],
                        role=role,
                        outcome=ShadowSearchOutcome.SUCCEEDED,
                        record_sha256="f" * 64,
                    )
                )
        with tempfile.TemporaryDirectory() as raw, SQLiteShadowSearchTelemetryStore(
            Path(raw) / "telemetry.sqlite3", binding=_binding()
        ) as store, mock.patch(
            "vdbench.shadow_search_telemetry.build_shadow_attempt_identity",
            side_effect=lambda group, *, trace_sequence_index: SimpleNamespace(
                attempt_sha256=f"{group[0].source_sequence // 50 + 9000:064x}"[-64:]
            ),
        ):
            store.records = lambda: tuple(records)
            summary = store.verify_prefix(tuple(sources))
            self.assertEqual(summary.record_count, 400)
            self.assertFalse(summary.complete)
            store.records = lambda: tuple(records[:-1])
            with self.assertRaises(ShadowSearchTelemetryError) as missing:
                store.verify_prefix(tuple(sources))
            self.assertEqual(missing.exception.code, "TELEMETRY_PREFIX_INVALID")


if __name__ == "__main__":
    unittest.main()
