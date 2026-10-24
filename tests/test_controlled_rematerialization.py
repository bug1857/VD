"""Offline ADR-026 foundations; no test creates a native client or authority.

Real DATASET-001 bytes and temporary SQLite stores exercise payload identity,
ownership, crash evidence and restart refusal. Fake ports are explicitly not
production P1-A/P1-B provenance or qualification evidence.
"""
from __future__ import annotations

import dataclasses
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

from vdbench.canonical_serialization import strict_canonical_json_bytes
from vdbench.controlled_lkg_control import (
    GATEWAYS, LIMITS, MAINTENANCE_SETTINGS, MEMBERS, WatchCoverage,
    gateway_configuration, protected_ranges, validate_profile,
)
from vdbench.controlled_lkg_milvus import PinnedColumnarMilvus, RPC_POLICY, verify_sdk_source
from vdbench.controlled_rematerialization import (
    CanonicalDataset001, ConstructionJournal, GenerationPlan, RematerializationError,
    digest, execute_construction, load_reserved_plan, reserve_generation,
    validate_input_arrays, verify_construction, verify_logical_values,
)
from vdbench.deployment_governance import resolve_deployment_governance_scope

ROOT = Path(__file__).resolve().parents[1]


def profile_and_origin():
    roles = sorted(set().union(*MEMBERS.values()))
    containers = {}
    for number, role in enumerate(roles, 1):
        networks = {network: f"172.{20 + list(MEMBERS).index(network)}.0.{number}"
                    for network, members in MEMBERS.items() if role in members}
        mounts = ([{"source": f"new-{role}-volume", "destination": f"/data/{role}",
                    "read_only": False, "type": "volume"}]
                  if role in {"milvus", "etcd", "minio"} else [])
        containers[role] = {"id": f"{number:064x}", "image": "sha256:" + "a" * 64,
                            "name": role, "networks": networks, "mounts": mounts,
                            "cpu_nanos": 1000000000, "memory_bytes": 536870912, "pids_limit": 128}
    gateways = {}
    for role, (front, initiator, backend, port) in GATEWAYS.items():
        gateways[role] = {"version": "1.30.5", "modules": ["stream", "stream_access"],
                         "configuration": gateway_configuration(
                             containers[role]["networks"][front],
                             containers[initiator]["networks"][front],
                             containers[backend]["networks"]["backend"], port)}
    profile = {"schema_version": "controlled-lkg-profile-v1", "containers": containers,
               "networks": {n: {"driver": "bridge", "internal": True, "ipv6": False,
                                 "members": sorted(m)} for n, m in MEMBERS.items()},
               "gateways": gateways, "principals": {r: r + "-test" for r in
                   ("materializer", "qualifier", "observer")},
               "maintenance": {k: False for k in MAINTENANCE_SETTINGS},
               "etcd": {"rootPath": "by-dev", "metaSubPath": "meta", "kvSubPath": "kv"},
               "limits": dict(LIMITS)}
    storage = {r: containers[r]["mounts"] for r in ("milvus", "etcd", "minio")}
    identities = ["runtime:" + containers[r]["id"] for r in storage]
    identities += ["storage:" + digest("storage-mount", m) for mounts in storage.values() for m in mounts]
    origin = {"schema_version": "fresh-controlled-origin-v1", "storage_mounts": storage,
              "identities": identities, "restored_from": [], "prior_materializations": [],
              "aliases": [], "routes": [], "allocation": {
                  "owner": "OFFLINE-TEST-ONLY", "request_sha256": "b" * 64,
                  "receipt_sha256": "c" * 64, "created_at_utc": "2026-10-05T00:00:00Z"}}
    return profile, origin


class FakeWriter:
    def __init__(self, failure=None):
        self.calls, self.closed, self.failure = [], False, failure

    def has_collection(self, name):
        return self.failure == "existing"

    def create_collection(self, name):
        self.calls.append(("CREATE", name))
        return len(self.calls)

    def insert(self, name, ids, vectors):
        self.calls.append(("INSERT", name, ids))
        if self.failure == "ambiguous":
            raise TimeoutError("effect may have happened")
        if self.failure == "crash":
            os._exit(17)
        return {"insert_count": 1000, "primary_keys": list(ids[:-1] if self.failure == "ack" else ids)}

    def flush(self, name):
        self.calls.append(("FLUSH", name))
        return {"segment_ids": [10, 11], "flush_timestamp": 100}

    def create_index(self, name, params):
        self.calls.append(("INDEX", name))

    def load(self, name):
        self.calls.append(("LOAD", name))

    def close(self):
        self.closed = True
        if self.failure == "close":
            raise OSError("session closure unavailable")


class ConstructionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = CanonicalDataset001.load(ROOT / "artifacts/exp-001/dataset")

    def plan(self, directory):
        profile, origin = profile_and_origin()
        return GenerationPlan.prepare(dataset=self.dataset, source_revision="a" * 40,
                   profile=profile, origin=origin,
                   scope=resolve_deployment_governance_scope(canonical_root=directory))

    def test_input_is_exact_immutable_and_unique(self):
        with self.assertRaises(ValueError):
            self.dataset.vectors.setflags(write=True)
        for ids, vectors in ((self.dataset.ids.astype("<i4"), self.dataset.vectors),
                             (self.dataset.ids, self.dataset.vectors.astype("<f8"))):
            with self.assertRaises(RematerializationError):
                validate_input_arrays(ids, vectors)
        ids = self.dataset.ids.copy()
        ids[1] = ids[0]
        with self.assertRaisesRegex(RematerializationError, "UNIQUENESS"):
            validate_input_arrays(ids, self.dataset.vectors)
        vectors = self.dataset.vectors.copy()
        vectors[0, 0] = np.nan
        with self.assertRaisesRegex(RematerializationError, "NONFINITE"):
            validate_input_arrays(self.dataset.ids, vectors)

    def test_success_replays_actual_sql_ownership_and_forbids_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            reserve_generation(plan, expected_plan_sha256=plan.sha256)
            writer, revoked = FakeWriter(), []
            execute_construction(plan, self.dataset, writer, revoke_writer=lambda: revoked.append(True),
                                 deadline_ns=100, clock=lambda: 1)
            self.assertTrue(writer.closed)
            self.assertEqual(revoked, [True])
            self.assertEqual(len(writer.calls), 28)
            with ConstructionJournal(plan, readonly=True) as journal:
                self.assertEqual(verify_construction(journal, self.dataset), {"FLAT": 1, "HNSW": 2})
                self.assertEqual(journal._connection.execute("SELECT COUNT(*) FROM owners").fetchone()[0], 20000)
            self.assertEqual(load_reserved_plan(plan.scope, plan.generation), plan)
            second = FakeWriter()
            with self.assertRaisesRegex(RematerializationError, "RESUME_REFUSED"):
                execute_construction(plan, self.dataset, second, revoke_writer=lambda: None,
                                     deadline_ns=100, clock=lambda: 1)
            self.assertEqual(second.calls, [])
            with ConstructionJournal(plan, readonly=True) as journal:
                with self.assertRaisesRegex(RematerializationError, "GENERATION_INVALID"):
                    verify_construction(journal, self.dataset)
            with self.assertRaisesRegex(RematerializationError, "ALREADY_CONSUMED"):
                reserve_generation(plan, expected_plan_sha256=plan.sha256)

    def test_ambiguous_mutation_bad_ack_deadline_and_revoke_failure_are_absorbing(self):
        for failure in ("ambiguous", "ack", "existing", "deadline", "revoke", "close"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                plan = self.plan(directory)
                reserve_generation(plan, expected_plan_sha256=plan.sha256)
                writer, revoked = FakeWriter(failure), []
                def revoke():
                    revoked.append(True)
                    if failure == "revoke":
                        raise OSError("revocation unavailable")
                with self.assertRaises((RematerializationError, TimeoutError, OSError)):
                    execute_construction(plan, self.dataset, writer, revoke_writer=revoke,
                                         deadline_ns=0 if failure == "deadline" else 100, clock=lambda: 1)
                self.assertTrue(writer.closed)
                self.assertEqual(revoked, [True])
                with ConstructionJournal(plan) as journal:
                    self.assertEqual(journal.events()[-1]["kind"], "INVALID")
                    with self.assertRaisesRegex(RematerializationError, "GENERATION_INVALID"):
                        journal.append("CONSTRUCTION_STARTED", {})
                self.assertLessEqual(sum(c[0] == "INSERT" for c in writer.calls),
                                     20 if failure in {"revoke", "close"} else 1)

    def test_durable_intent_survives_process_crash_without_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            reserve_generation(plan, expected_plan_sha256=plan.sha256)
            code = """
import sys
from test_controlled_rematerialization import FakeWriter
from vdbench.controlled_rematerialization import *
from vdbench.deployment_governance import resolve_deployment_governance_scope
s=resolve_deployment_governance_scope(canonical_root=sys.argv[1])
p=load_reserved_plan(s,sys.argv[2])
d=CanonicalDataset001.load(sys.argv[3])
execute_construction(p,d,FakeWriter('crash'),revoke_writer=lambda:None,deadline_ns=100,clock=lambda:1)
"""
            environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": f"{ROOT / 'src'}:{ROOT / 'tests'}"}
            result = subprocess.run([sys.executable, "-c", code, directory, plan.generation,
                                     str(ROOT / "artifacts/exp-001/dataset")], env=environment,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 17, result.stderr)
            with ConstructionJournal(plan, readonly=True) as journal:
                self.assertEqual(journal.events()[-1]["kind"], "EFFECT_INTENT")
                with self.assertRaises(RematerializationError):
                    verify_construction(journal, self.dataset)
            with self.assertRaises(RematerializationError):
                execute_construction(plan, self.dataset, FakeWriter(), revoke_writer=lambda: None,
                                     deadline_ns=100, clock=lambda: 1)

    def test_existing_empty_registry_is_not_repaired(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            # Governance scope must have its usual private ancestors as well.
            from vdbench.deployment_governance import ensure_deployment_scope_directory
            ensure_deployment_scope_directory(plan.scope)
            plan.root.mkdir(mode=0o700)
            path = plan.root / "registry.sqlite3"
            path.touch(mode=0o600)
            before = path.read_bytes()
            with self.assertRaisesRegex(RematerializationError, "SCHEMA_INVALID"):
                reserve_generation(plan, expected_plan_sha256=plan.sha256)
            self.assertEqual(path.read_bytes(), before)

    def test_wal_registry_is_refused_without_normalization_or_schema_repair(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            from vdbench.deployment_governance import ensure_deployment_scope_directory
            ensure_deployment_scope_directory(plan.scope)
            plan.root.mkdir(mode=0o700)
            path = plan.root / "registry.sqlite3"
            path.touch(mode=0o600)
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("CREATE TABLE unrelated (value TEXT)")
            connection.commit()
            connection.close()
            before = {p.name: p.read_bytes() for p in plan.root.iterdir()}
            with self.assertRaisesRegex(RematerializationError, "JOURNAL_MODE_INVALID"):
                reserve_generation(plan, expected_plan_sha256=plan.sha256)
            self.assertEqual({p.name: p.read_bytes() for p in plan.root.iterdir()}, before)
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            finally:
                connection.close()

    def test_payload_drift_is_refused_before_effect(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            doc = plan.document
            doc["requests"][0]["payload_sha256"] = "f" * 64
            doc["request_manifest_sha256"] = digest("requests", doc["requests"])
            altered = dataclasses.replace(plan, document_bytes=strict_canonical_json_bytes(doc))
            writer = FakeWriter()
            with self.assertRaisesRegex(RematerializationError, "PAYLOAD_CHANGED"):
                execute_construction(altered, self.dataset, writer, revoke_writer=lambda: None,
                                     deadline_ns=100, clock=lambda: 1)
            self.assertEqual(writer.calls, [])

    def test_native_adapter_cannot_enter_executor_without_control_and_supervisor(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            writer = object.__new__(PinnedColumnarMilvus)
            writer.close = mock.Mock()
            revoked = mock.Mock()
            with self.assertRaisesRegex(RematerializationError, "NATIVE_CONTROL_AND_SUPERVISOR_REQUIRED"):
                execute_construction(plan, self.dataset, writer, revoke_writer=revoked,
                                     deadline_ns=100, clock=lambda: 1)
            writer.close.assert_called_once_with()
            revoked.assert_called_once_with()
            self.assertFalse(plan.journal_path.exists())

    def test_journal_sync_failure_prevents_dispatch_and_latches_invalidation(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            reserve_generation(plan, expected_plan_sha256=plan.sha256)
            writer, revoked = FakeWriter(), []
            with mock.patch("vdbench.controlled_rematerialization._sync_directory", side_effect=OSError("fsync failed")):
                with self.assertRaises(OSError):
                    execute_construction(plan, self.dataset, writer, revoke_writer=lambda: revoked.append(True),
                                         deadline_ns=100, clock=lambda: 1)
            self.assertEqual(writer.calls, [])
            self.assertEqual(revoked, [True])
            self.assertTrue(writer.closed)
            with ConstructionJournal(plan, readonly=True) as journal:
                self.assertEqual(journal.events()[-1]["kind"], "INVALID")

    def test_storage_and_runtime_origins_are_consumed_across_fresh_plans(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = self.plan(directory), self.plan(directory)
            self.assertNotEqual(first.generation, second.generation)
            reserve_generation(first, expected_plan_sha256=first.sha256)
            with self.assertRaisesRegex(RematerializationError, "ALREADY_CONSUMED"):
                reserve_generation(second, expected_plan_sha256=second.sha256)
            self.assertFalse(second.journal_path.exists())

    def test_ownership_table_tamper_is_not_hidden_by_valid_event_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = self.plan(directory)
            reserve_generation(plan, expected_plan_sha256=plan.sha256)
            execute_construction(plan, self.dataset, FakeWriter(), revoke_writer=lambda: None,
                                 deadline_ns=100, clock=lambda: 1)
            connection = sqlite3.connect(plan.journal_path)
            try:
                sql = connection.execute("SELECT sql FROM sqlite_master WHERE name='owners_no_delete'").fetchone()[0]
                connection.execute("DROP TRIGGER owners_no_delete")
                connection.execute("DELETE FROM owners WHERE target='FLAT' AND pk=0")
                connection.execute(sql)
                connection.commit()
            finally:
                connection.close()
            with ConstructionJournal(plan, readonly=True) as journal:
                with self.assertRaisesRegex(RematerializationError, "APPLICATION_OWNERSHIP_INVALID"):
                    verify_construction(journal, self.dataset)


class LogicalValueTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = CanonicalDataset001.load(ROOT / "artifacts/exp-001/dataset")

    def reader(self, fault=None):
        data = self.dataset
        class Reader:
            def query(self, name, expression, fields):
                if expression == "id < 0":
                    return [{"id": -1}] if fault == "extra" else []
                if expression == "id >= 0":
                    return [{"id": p} for p in range(9999 if fault == "missing_domain" else 10000)]
                pk = int(expression.split(" == ")[1])
                if pk == 0 and fault == "missing_value":
                    return []
                vector = data.vectors[pk].tolist()
                if pk == 0 and fault == "wrong":
                    vector[0] = float(np.nextafter(data.vectors[pk, 0], np.float32(np.inf)))
                if pk == 0 and fault == "rounded":
                    vector[0] += 1e-15
                return [{"id": pk, "vector": vector}, {"id": pk, "vector": list(vector)}]
        return Reader()

    def test_identical_copies_are_permitted_and_exact_canonical_values_checked(self):
        result = verify_logical_values(self.reader(), "offline", self.dataset, check=lambda: None)
        self.assertEqual(result["pk_count"], 10000)
        self.assertEqual(result["predicate"], "nonempty-canonical-value-set")

    def test_missing_extra_and_noncanonical_visible_values_refuse(self):
        for fault in ("extra", "missing_domain", "missing_value", "wrong", "rounded"):
            with self.subTest(fault=fault), self.assertRaises(RematerializationError):
                verify_logical_values(self.reader(fault), "offline", self.dataset, check=lambda: None)


class CoverageTests(unittest.TestCase):
    def coverage(self):
        self.records, self.invalid, self.now = [], [], 1
        return WatchCoverage(etcd={"rootPath": "by-dev", "metaSubPath": "meta", "kvSubPath": "kv"},
                             revision=7, record=lambda k, v: self.records.append((k, v)) or "a" * 64,
                             invalidate=self.invalid.append, clock_ns=lambda: self.now)

    def complete(self, coverage):
        for i, (start, end) in enumerate(coverage.ranges):
            coverage.snapshot_page(i, {"request_revision": 7, "header_revision": 9,
                                     "start": start, "end": end, "more": False, "kvs": []})
            coverage.watch_created(i, start_revision=8, prev_kv=True)

    def test_q_is_exact_byte_prefix_and_union_overlaps_are_merged(self):
        ranges = protected_ranges({"rootPath": "by-dev", "metaSubPath": "meta", "kvSubPath": "kv"})
        self.assertIn((b"by-dev/config", b"by-dev/confih"), ranges)
        self.assertEqual(len(protected_ranges({"rootPath": "by-dev", "metaSubPath": "meta", "kvSubPath": "meta/a"})), 2)

    def test_header_does_not_advance_processed_progress(self):
        c = self.coverage()
        self.complete(c)
        with self.assertRaisesRegex(RematerializationError, "BARRIER_INCOMPLETE"):
            c.barrier(9)
        self.assertEqual(self.invalid, ["WATCH_BARRIER_INCOMPLETE"])
        with self.assertRaises(RematerializationError):
            c.progress(0, revision=9)

    def test_progress_is_durable_and_all_ranges_are_required(self):
        c = self.coverage()
        self.complete(c)
        for i in range(len(c.ranges)):
            c.progress(i, revision=10)
        value = c.barrier(10)
        strict_canonical_json_bytes(value)
        self.assertEqual(len(value["processed_watermarks"]), len(c.ranges))
        self.now += LIMITS["progress_deadline_ns"] + 1
        with self.assertRaisesRegex(RematerializationError, "DEADLINE"):
            c.check()

    def test_same_value_q_write_malformed_event_and_watch_resume_latch(self):
        for fault in ("q", "malformed", "resume", "pending", "fragment"):
            c = self.coverage()
            self.complete(c)
            with self.subTest(fault=fault), self.assertRaises(RematerializationError):
                if fault == "resume":
                    c.watch_created(0, start_revision=8, prev_kv=True)
                elif fault == "pending":
                    c.progress(0, revision=8, events_pending=1)
                else:
                    index = next(i for i, (s, e) in enumerate(c.ranges) if s == b"by-dev/config")
                    event = {"type": "PUT", "revision": 8, "key": b"by-dev/configX",
                             "value": b"same", "previous": b"same"}
                    if fault == "malformed":
                        del event["previous"]
                    c.events(index, events=[event], fragmented=fault == "fragment")
            self.assertTrue(self.invalid)
            with self.assertRaises(RematerializationError):
                c.check()
            with self.assertRaises(RematerializationError):
                _ = c.snapshot

    def test_journal_failure_does_not_advance_watermark(self):
        c = self.coverage()
        self.complete(c)
        c._record = mock.Mock(side_effect=OSError("fsync failed"))
        with self.assertRaises(RematerializationError):
            c.progress(0, revision=10)
        self.assertEqual(c._watermarks[0], 7)
        self.assertEqual(self.invalid, ["WATCH_JOURNAL_DURABILITY_FAILED"])

    def test_profile_rejects_added_network_mutable_worker_mount_and_gateway_failover(self):
        for fault in ("network", "mount", "gateway"):
            p, _o = profile_and_origin()
            if fault == "network":
                p["containers"]["worker"]["networks"]["default"] = "172.30.0.10"
            elif fault == "mount":
                p["containers"]["worker"]["mounts"] = [{"source": "old-governance", "destination": "/input/dataset-001",
                                                         "read_only": False, "type": "bind"}]
            else:
                p["gateways"]["qualification_gateway"]["configuration"] += "upstream failover {}"
            with self.subTest(fault=fault), self.assertRaises(RematerializationError):
                validate_profile(p)


class SdkPolicyTests(unittest.TestCase):
    def test_actual_sdk_query_decodes_binary32_without_rounding_other_types(self):
        from pymilvus.client.grpc_handler import GrpcHandler
        from pymilvus.grpc_gen import milvus_pb2, schema_pb2

        dataset = CanonicalDataset001.load(ROOT / "artifacts/exp-001/dataset")
        response = milvus_pb2.QueryResults()
        ids = response.fields_data.add(type=schema_pb2.Int64, field_name="id", field_id=100)
        ids.scalars.long_data.data.append(0)
        vector = response.fields_data.add(type=schema_pb2.FloatVector, field_name="vector", field_id=101)
        vector.vectors.dim = 128
        vector.vectors.float_vector.data.extend(dataset.vectors[0].tolist())
        handler = object.__new__(GrpcHandler)
        handler._address = "offline-no-connection:19530"
        handler._stub = mock.Mock()
        handler._stub.Query.return_value = response
        reader = object.__new__(PinnedColumnarMilvus)
        reader._handler, reader._context = handler, None
        reader.check = lambda: None
        result = reader.query("offline", "id == 0", ["id", "vector"])
        self.assertEqual(result, [{"id": 0, "vector": dataset.vectors[0].tolist()}])
        request = handler._stub.Query.call_args.args[0]
        self.assertFalse(any(p.key in {"limit", "offset"} for p in request.query_params))
        self.assertEqual(list(request.partition_names), [])
        self.assertEqual(request.consistency_level, 0)  # Native Strong enum.
        reader._call = mock.Mock(return_value=[{"id": 0, "vector": dataset.vectors[0].astype("<f8")}])
        with self.assertRaisesRegex(RematerializationError, "NATIVE_VECTOR_ENCODING_INVALID"):
            reader.query("offline", "id == 0", ["id", "vector"])

    def test_actual_sdk_columnar_encoding_and_rpc_failure_send_one_request(self):
        import grpc
        from pymilvus import Collection, CollectionSchema, FieldSchema, DataType
        from pymilvus.client.grpc_handler import GrpcHandler
        from pymilvus.grpc_gen import milvus_pb2
        from pymilvus.exceptions import MilvusException

        class Unavailable(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE
            def details(self):
                return "offline injected ambiguous acknowledgment"

        schema = CollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema("vector", DataType.FLOAT_VECTOR, dim=128)], enable_dynamic_field=False)
        handler = object.__new__(GrpcHandler)
        handler._address = "offline-no-connection:19530"
        handler._stub = mock.Mock()
        collection = object.__new__(Collection)
        collection._schema, collection._schema_dict = schema, schema.to_dict()
        collection._name = "offline"
        collection._get_connection = lambda **kwargs: (handler, None)
        writer = object.__new__(PinnedColumnarMilvus)
        writer.check = lambda: None
        writer._collection = lambda name: collection
        vectors = CanonicalDataset001.load(ROOT / "artifacts/exp-001/dataset").vectors[:2]
        for error in (Unavailable(), TimeoutError("offline lost ack")):
            with self.subTest(error=type(error).__name__):
                handler._stub.Insert.future.reset_mock()
                handler._stub.Insert.future.return_value.result.side_effect = error
                with mock.patch("pymilvus.decorators.LOGGER"), self.assertRaises(MilvusException):
                    writer.insert("offline", (0, 1), vectors)
                self.assertEqual(handler._stub.Insert.future.call_count, 1)
                args, kwargs = handler._stub.Insert.future.call_args
                request = args[0]
                self.assertIsInstance(request, milvus_pb2.InsertRequest)
                self.assertEqual(request.num_rows, 2)
                self.assertEqual(list(request.fields_data[0].scalars.long_data.data), [0, 1])
                actual = np.asarray(request.fields_data[1].vectors.float_vector.data, dtype="<f4")
                self.assertEqual(actual.tobytes(), vectors.tobytes())
                self.assertIsNone(kwargs["timeout"])
                handler._stub.Insert.assert_not_called()

    def test_exact_installed_source_hashes_without_connecting(self):
        self.assertEqual(len(verify_sdk_source()), 12)

    def test_columnar_insert_keeps_exact_arrays_and_disables_sdk_retries(self):
        writer = object.__new__(PinnedColumnarMilvus)
        writer.check = lambda: None
        collection = mock.Mock()
        collection.insert.return_value = mock.Mock(insert_count=2, primary_keys=[0, 1])
        writer._collection = lambda name: collection
        vectors = np.zeros((2, 128), dtype="<f4")
        self.assertEqual(writer.insert("offline", (0, 1), vectors), {"insert_count": 2, "primary_keys": [0, 1]})
        args, kwargs = collection.insert.call_args
        self.assertEqual(args[0][0], [0, 1])
        self.assertIs(args[0][1], vectors)
        self.assertEqual(kwargs, RPC_POLICY)

    def test_logical_query_omits_limit_offset_and_partition_selection(self):
        reader = object.__new__(PinnedColumnarMilvus)
        reader.check = lambda: None
        reader._call = mock.Mock(return_value=[{"id": 1}])
        self.assertEqual(reader.query("offline", "id >= 0", ["id"]), [{"id": 1}])
        reader._call.assert_called_once_with("query", "offline", "id >= 0", output_fields=["id"],
                                            consistency_level="Strong", strict_float32=True)


if __name__ == "__main__":
    unittest.main()
