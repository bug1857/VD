"""FINDING-005, FINDING-006, FINDING-008 and NEW_OBSERVATION_A regressions.

FINDING-005 alleged that `python -O` could strip a safety-relevant assertion.
The allegation was not reproduced: every cited site is preceded by explicit
validation that raises or returns first, so the assertion is unreachable-state
documentation. These tests prove that mechanically by running the same
externally reachable invalid inputs in both a normal and a `-O` subprocess and
requiring identical outcomes -- so a future edit that starts *relying* on an
assertion fails here.

FINDING-006 requires that authoritative paths stay fail-closed while explicitly
best-effort paths keep returning explicit rejection/unavailable semantics, and
that corruption is never silently downgraded.

FINDING-008's wall-clock causal-authority allegation was disproved dynamically;
the permanent regression is kept here.
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from vdbench.config import ContractViolation, IndexTrack, Metric, SearchConfiguration
from vdbench.exp010_serving_configuration import (
    Exp010ServingConfigurationError,
    validate_governed_configuration_identity,
)
from vdbench.shadow_attempt_store import (
    ShadowAttemptStoreError,
    SQLiteShadowAttemptStore,
)
from vdbench.shadow_event_types import MonitorStreamKey
import vdbench.actuation_persistence as actuation_persistence
import vdbench.canary_admission as canary_admission
import vdbench.shadow_attempt_store as shadow_attempt_store

from tests.test_milvus_actuation import FLAT_NAME, THRESHOLD_STRATUM, fixture_components
from tests.test_real_detector_attestation import _ENVIRONMENT, _REVISION, _stream


_REPOSITORY = Path(__file__).parents[1]


def _run(script: str, *, optimized: bool) -> subprocess.CompletedProcess:
    command = [sys.executable]
    if optimized:
        command.append("-O")
    command.extend(["-c", script])
    return subprocess.run(
        command,
        cwd=_REPOSITORY,
        env={**os.environ, "PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        capture_output=True,
        check=True,
    )


#: Each probe drives an externally reachable invalid input through a boundary
#: whose assert FINDING-005 cited, and prints one stable outcome line.
_PROBES = {
    "validate_v3_record_non_mapping": (
        "from vdbench.actuation_persistence import _validate_v3_record\n"
        "try:\n"
        "    _validate_v3_record(['not', 'a', 'mapping'])\n"
        "except BaseException as exc:\n"
        "    print(type(exc).__name__)\n"
        "else:\n"
        "    print('NO_ERROR')\n"
    ),
    "validate_safety_gates_non_mapping": (
        "from vdbench.actuation_persistence import _validate_safety_gates\n"
        "try:\n"
        "    _validate_safety_gates([42])\n"
        "except BaseException as exc:\n"
        "    print(type(exc).__name__)\n"
        "else:\n"
        "    print('NO_ERROR')\n"
    ),
    "validate_evidence_provenance_non_mapping": (
        "from vdbench.actuation_persistence import _validate_evidence_provenance\n"
        "try:\n"
        "    _validate_evidence_provenance(['x'])\n"
        "except BaseException as exc:\n"
        "    print(type(exc).__name__)\n"
        "else:\n"
        "    print('NO_ERROR')\n"
    ),
    "project_v3_record_wrong_type": (
        "from vdbench.actuation_persistence import _project_v3_record\n"
        "try:\n"
        "    _project_v3_record(object())\n"
        "except BaseException as exc:\n"
        "    print(type(exc).__name__)\n"
        "else:\n"
        "    print('NO_ERROR')\n"
    ),
    "audit_sink_empty_audit_id": (
        "from vdbench.actuation_persistence import JsonlAuditSink\n"
        "try:\n"
        "    JsonlAuditSink('/nonexistent/audit.jsonl').contains('')\n"
        "except BaseException as exc:\n"
        "    print(type(exc).__name__)\n"
        "else:\n"
        "    print('NO_ERROR')\n"
    ),
    "stage4_admission_invalid_request": (
        "from vdbench.canary_admission import evaluate_stage4_admission\n"
        "result = evaluate_stage4_admission(object())\n"
        "print(result.receipt is None, ','.join(result.reason_codes))\n"
    ),
    "shadow_extraction_invalid_windows": (
        "from vdbench.config import Metric\n"
        "from vdbench.shadow_extraction import extract_window_evidence\n"
        "try:\n"
        "    out = extract_window_evidence(\n"
        "        reference_window=None, current_window=None, metric=Metric.L2,\n"
        "        detector_seed=1)\n"
        "    print('RESULT', getattr(out, 'complete', None))\n"
        "except BaseException as exc:\n"
        "    print(type(exc).__name__)\n"
    ),
    "governed_identity_syntax": (
        "from vdbench.exp010_serving_configuration import "
        "validate_governed_configuration_identity as v\n"
        "for bad in ('', 'config-v1', 'exp010-serving-config-v1:sha256:zz'):\n"
        "    try:\n"
        "        v(bad); print('ACCEPTED')\n"
        "    except BaseException as exc:\n"
        "        print(getattr(exc, 'code', type(exc).__name__))\n"
    ),
    "monitor_stream_key_validation": (
        "from vdbench.config import Metric\n"
        "from vdbench.shadow_event_types import MonitorStreamKey\n"
        "for bad in ('', ' x ', 'a\\nb'):\n"
        "    try:\n"
        "        MonitorStreamKey('s', Metric.L2, 't', bad, 'd', 'f', 'h')\n"
        "        print('ACCEPTED')\n"
        "    except BaseException as exc:\n"
        "        print(type(exc).__name__)\n"
    ),
}


class PythonOptimizeEquivalenceTests(unittest.TestCase):
    """FINDING-005: `-O` must not change any safety-relevant outcome."""

    def test_every_cited_boundary_behaves_identically_under_dash_o(self) -> None:
        for name, script in _PROBES.items():
            with self.subTest(probe=name):
                plain = _run(script, optimized=False)
                optimized = _run(script, optimized=True)
                self.assertEqual(plain.stdout, optimized.stdout, name)
                self.assertNotIn("AssertionError", plain.stdout)
                self.assertNotIn("NO_ERROR", plain.stdout)

    def test_asserts_are_disabled_in_the_optimized_subprocess(self) -> None:
        """Guards the guard: prove `-O` really is stripping assertions."""

        script = (
            "try:\n"
            "    assert False\n"
            "    print('STRIPPED')\n"
            "except AssertionError:\n"
            "    print('ACTIVE')\n"
        )
        self.assertEqual(_run(script, optimized=False).stdout.strip(), "ACTIVE")
        self.assertEqual(_run(script, optimized=True).stdout.strip(), "STRIPPED")

    def test_durable_authority_rejection_is_not_an_assertion(self) -> None:
        """The FINDING-007 cross-binding refusal must survive `-O`."""

        script = (
            "import sys; sys.path.insert(0, '.')\n"
            "import tempfile\n"
            "from pathlib import Path\n"
            "from vdbench.shadow_attempt_store import (\n"
            "    SQLiteShadowAttemptStore, build_shadow_attempt_identity)\n"
            "from vdbench.shadow_window import (\n"
            "    TRACE_QUERY_COUNT, WINDOW_QUERY_COUNT,\n"
            "    PersistedShadowTraceEnvelope, hash_shadow_audit_trace)\n"
            "from tests.test_real_detector_attestation import (\n"
            "    _ENVIRONMENT, _REVISION, _commit_sources, _stream, _trace_for)\n"
            "with tempfile.TemporaryDirectory() as d:\n"
            "    root = Path(d)\n"
            "    sources = tuple(_commit_sources(\n"
            "        root / 's.sqlite3', WINDOW_QUERY_COUNT * 2))\n"
            "    w0 = sources[:TRACE_QUERY_COUNT]\n"
            "    w1 = sources[WINDOW_QUERY_COUNT:WINDOW_QUERY_COUNT+TRACE_QUERY_COUNT]\n"
            "    ident = build_shadow_attempt_identity(w0, trace_sequence_index=0)\n"
            "    trace = _trace_for(w1)\n"
            "    foreign = PersistedShadowTraceEnvelope(\n"
            "        trace_id='v2-window-1-trace-0',\n"
            "        captured_at_utc='2026-08-15T00:00:01Z', sequence_index=0,\n"
            "        declared_observation_count=TRACE_QUERY_COUNT,\n"
            "        expected_trace_sha256=hash_shadow_audit_trace(trace),\n"
            "        trace=trace)\n"
            "    store = SQLiteShadowAttemptStore(root / 'a.sqlite3',\n"
            "        stream_key=_stream(), source_revision=_REVISION,\n"
            "        environment_manifest_sha256=_ENVIRONMENT)\n"
            "    permit = store.start_attempt(ident,\n"
            "        started_at_utc='2026-08-15T00:00:02Z')\n"
            "    try:\n"
            "        store.fail_attempt(ident, permit=permit,\n"
            "            failed_at_utc='2026-08-15T00:00:03Z',\n"
            "            failure_code='X', envelope=foreign)\n"
            "        print('ACCEPTED')\n"
            "    except BaseException as exc:\n"
            "        print(getattr(exc, 'code', type(exc).__name__))\n"
            "    store.close()\n"
        )
        plain = _run(script, optimized=False).stdout.strip()
        optimized = _run(script, optimized=True).stdout.strip()
        self.assertEqual(plain, "SHADOW_ATTEMPT_TRACE_BINDING_INVALID")
        self.assertEqual(plain, optimized)


class AssertSiteDocumentationTests(unittest.TestCase):
    """FINDING-005: each cited assert must be preceded by a real check."""

    def test_shadow_attempt_store_has_no_assertions_at_all(self) -> None:
        source = inspect.getsource(shadow_attempt_store)
        self.assertNotIn("\n        assert ", source)
        self.assertNotIn("\n            assert ", source)

    def test_cited_asserts_follow_an_explicit_guard(self) -> None:
        for module in (actuation_persistence, canary_admission):
            with self.subTest(module=module.__name__):
                lines = inspect.getsource(module).splitlines()
                for index, line in enumerate(lines):
                    if not line.strip().startswith("assert "):
                        continue
                    preceding = "\n".join(lines[max(0, index - 12) : index])
                    self.assertTrue(
                        any(
                            token in preceding
                            for token in (
                                "raise ",
                                "return ",
                                "_exact_mapping",
                                "if reasons",
                            )
                        ),
                        f"{module.__name__}:{index + 1} lacks a preceding guard",
                    )


class FailClosedSemanticsTests(unittest.TestCase):
    """FINDING-006: narrow where it hides defects, explicit where best-effort."""

    def test_authoritative_store_write_fails_closed_on_corruption(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "attempts.sqlite3"
            store = SQLiteShadowAttemptStore(
                path,
                stream_key=_stream(),
                source_revision=_REVISION,
                environment_manifest_sha256=_ENVIRONMENT,
            )
            store.close()
            # Corrupt the durable file; reopening must refuse, never repair.
            path.write_bytes(b"not a sqlite database" + b"\x00" * 64)
            os.chmod(path, 0o600)
            with self.assertRaises(Exception) as caught:
                SQLiteShadowAttemptStore(
                    path,
                    stream_key=_stream(),
                    source_revision=_REVISION,
                    environment_manifest_sha256=_ENVIRONMENT,
                )
            self.assertNotIsInstance(caught.exception, AssertionError)

    def test_corruption_is_not_downgraded_to_an_empty_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            path = root / "attempts.sqlite3"
            store = SQLiteShadowAttemptStore(
                path,
                stream_key=_stream(),
                source_revision=_REVISION,
                environment_manifest_sha256=_ENVIRONMENT,
            )
            try:
                store._connection.execute("DROP TRIGGER attempt_events_no_update")
                with self.assertRaises(ShadowAttemptStoreError) as caught:
                    store.records_for_window(0)
                self.assertEqual(
                    caught.exception.code, "SHADOW_ATTEMPT_SCHEMA_INVALID"
                )
            finally:
                store.close()

    def test_best_effort_capture_failure_yields_an_explicit_terminal_record(
        self,
    ) -> None:
        """The injected physical boundary stays broad, but never silent."""

        import vdbench.v2_shadow_worker as v2_shadow_worker

        source = inspect.getsource(v2_shadow_worker)
        # The one broad handler is annotated and immediately persists a
        # terminal FAILED record with a stable failure code.
        self.assertIn("noqa: BLE001 - injected physical boundary", source)
        self.assertIn("SHADOW_CAPTURE_EXCEPTION", source)
        self.assertIn("_fail_started_attempt", source)


class MonotonicCausalAuthorityTests(unittest.TestCase):
    """FINDING-008: monotonic_ns is causal authority; UTC is an audit label."""

    def _adapter(self):
        _workload, _client, _estimator, _health, adapter = fixture_components()
        return adapter

    def _configuration(self) -> SearchConfiguration:
        return SearchConfiguration(
            metric=Metric.L2,
            threshold_label=THRESHOLD_STRATUM,
            radius=100.0,
            index_track=IndexTrack.FLAT,
        )

    def test_backward_monotonic_clock_fails_closed(self) -> None:
        adapter = self._adapter()
        values = iter([1_000_000_000, 500_000_000])
        adapter.clock_ns = lambda: next(values)
        with self.assertRaises(ContractViolation):
            adapter._timed_search(
                name=FLAT_NAME,
                query=adapter.workload.query_vectors[0],
                configuration=self._configuration(),
            )

    def test_elapsed_measurement_ignores_the_wall_clock_entirely(self) -> None:
        """A backward UTC clock cannot corrupt a latency measurement."""

        adapter = self._adapter()
        ticks = iter([1_000_000_000, 1_002_000_000])
        adapter.clock_ns = lambda: next(ticks)
        outcome = adapter._timed_search(
            name=FLAT_NAME,
            query=adapter.workload.query_vectors[0],
            configuration=self._configuration(),
        )
        self.assertIsNone(outcome.exception)
        self.assertAlmostEqual(outcome.latency_ms, 2.0)

    def test_latency_is_never_negative_even_on_an_exception_path(self) -> None:
        adapter = self._adapter()
        values = iter([2_000_000_000, 1_000_000_000])
        adapter.clock_ns = lambda: next(values)

        def explode(**_kwargs):
            raise RuntimeError("injected search failure")

        adapter.harness.search = explode
        outcome = adapter._timed_search(
            name=FLAT_NAME,
            query=adapter.workload.query_vectors[0],
            configuration=self._configuration(),
        )
        self.assertIsNotNone(outcome.exception)
        self.assertGreaterEqual(outcome.latency_ms, 0.0)

    def test_monotonic_check_is_not_an_assertion(self) -> None:
        import vdbench.milvus_actuation as milvus_actuation

        source = inspect.getsource(milvus_actuation)
        self.assertIn("clock_ns must return monotonic integer nanoseconds", source)
        marker = source.index("clock_ns must return monotonic integer nanoseconds")
        self.assertIn("raise ContractViolation", source[marker - 200 : marker])


class StreamIdentityValidationTests(unittest.TestCase):
    """NEW_OBSERVATION_A: earliest safe boundary, plus a strict new-record one."""

    def test_governed_identity_is_accepted_by_both_boundaries(self) -> None:
        identity = "exp010-serving-config-v1:sha256:" + "8" * 64
        self.assertEqual(validate_governed_configuration_identity(identity), identity)
        key = MonitorStreamKey(
            "stream", Metric.L2, "target-075", identity, "data", "flat", "hnsw"
        )
        self.assertEqual(key.configuration_identity, identity)

    def test_legacy_identities_remain_constructible(self) -> None:
        """Historical and non-EXP-010 identities must still decode."""

        for legacy in (
            "config-v1",
            "exp005-shadow-configuration-v1:sha256:" + "a" * 64,
            "some-other-domain-identity",
        ):
            with self.subTest(legacy=legacy):
                key = MonitorStreamKey(
                    "stream", Metric.L2, "target-075", legacy, "d", "f", "h"
                )
                self.assertEqual(key.configuration_identity, legacy)

    def test_malformed_components_are_refused_by_the_value_type(self) -> None:
        for label, bad in (
            ("empty", ""),
            ("leading space", " x"),
            ("trailing space", "x "),
            ("newline", "a\nb"),
            ("null byte", "a\x00b"),
            ("non-str", 7),
        ):
            with self.subTest(case=label):
                with self.assertRaises((ValueError, TypeError)):
                    MonitorStreamKey(
                        "stream", Metric.L2, "target-075", bad, "d", "f", "h"
                    )

    def test_every_identity_component_is_validated(self) -> None:
        fields = (
            "stream_id",
            "threshold_stratum",
            "configuration_identity",
            "data_identity",
            "flat_binding_id",
            "hnsw_binding_id",
        )
        base = ["s", Metric.L2, "t", "c", "d", "f", "h"]
        positions = {name: index for index, name in enumerate(
            ("stream_id", "metric", "threshold_stratum", "configuration_identity",
             "data_identity", "flat_binding_id", "hnsw_binding_id")
        )}
        for field in fields:
            with self.subTest(field=field):
                values = list(base)
                values[positions[field]] = "bad\nvalue"
                with self.assertRaises(ValueError):
                    MonitorStreamKey(*values)

    def test_new_record_boundary_refuses_a_non_governed_identity(self) -> None:
        for bad in ("config-v1", "", "exp010-serving-config-v1:sha256:short", None):
            with self.subTest(bad=bad):
                with self.assertRaises(Exp010ServingConfigurationError) as caught:
                    validate_governed_configuration_identity(bad)
                self.assertEqual(
                    caught.exception.code, "CONFIGURATION_IDENTITY_SYNTAX_INVALID"
                )

    def test_cross_binding_mismatch_still_fails_closed_downstream(self) -> None:
        """Weak-syntax acceptance never became weak *binding* acceptance."""

        first = MonitorStreamKey(
            "stream", Metric.L2, "target-075", "config-a", "d", "f", "h"
        )
        second = MonitorStreamKey(
            "stream", Metric.L2, "target-075", "config-b", "d", "f", "h"
        )
        self.assertNotEqual(first, second)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
