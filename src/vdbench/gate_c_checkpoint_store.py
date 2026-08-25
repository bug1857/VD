"""Append-only, non-authorizing audit ledger for bounded Gate-C checkpoints.

Canonical Gate-C acknowledgements, attempts, detector/attestation records,
finalization state, and telemetry remain execution truth.  This ledger records
only that an exact bounded envelope was started and that canonical state later
reconstructively satisfied its checkpoint result.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import stat
import threading
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Self

from .canonical_serialization import strict_canonical_digest, strict_canonical_json_bytes
from .gate_c_bounded_execution import (
    GateCBoundedExecutionEnvelope,
    GateCBoundedExecutionEnvelopeV2,
    gate_c_bounded_execution_envelope_document,
    gate_c_bounded_execution_envelope_document_v2,
    parse_gate_c_bounded_execution_envelope_document,
    parse_gate_c_bounded_execution_envelope_document_v2,
    verify_gate_c_checkpoint_result,
    verify_gate_c_checkpoint_result_v2,
)

__all__ = [
    "GateCCheckpointEventKind",
    "GateCCheckpointLedgerBinding",
    "GateCCheckpointLedgerError",
    "GateCCheckpointLedgerState",
    "SQLiteGateCCheckpointLedger",
]


_BINDING_SCHEMA = "exp012-scale-gate-c-checkpoint-binding-v1"
_EVENT_SCHEMA = "exp012-scale-gate-c-checkpoint-event-v1"
_EVENT_SCHEMA_V2 = "exp012-scale-gate-c-checkpoint-event-v2"
_BINDING_DOMAIN = b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_BINDING::V1\x00"
_EVENT_DOMAIN = b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_EVENT::V1\x00"
_EVENT_DOMAIN_V2 = b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_EVENT::V2\x00"
_SHA256 = re.compile(r"[0-9a-f]{64}")


class GateCCheckpointLedgerError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> GateCCheckpointLedgerError:
    return GateCCheckpointLedgerError(code)


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _error("GATE_C_CHECKPOINT_LEDGER_VALUE_INVALID")
    return value


def _text(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _error("GATE_C_CHECKPOINT_LEDGER_VALUE_INVALID")
    return value


class GateCCheckpointEventKind(StrEnum):
    CHECKPOINT_STARTED = "CHECKPOINT_STARTED"
    CHECKPOINT_COMPLETED = "CHECKPOINT_COMPLETED"


@dataclass(frozen=True, slots=True)
class GateCCheckpointLedgerBinding:
    campaign_identity: str
    campaign_binding_sha256: str
    scale_contract_sha256: str
    source_revision: str

    def __post_init__(self) -> None:
        _text(self.campaign_identity)
        _sha(self.campaign_binding_sha256)
        _sha(self.scale_contract_sha256)
        _text(self.source_revision)


@dataclass(frozen=True, slots=True)
class GateCCheckpointLedgerState:
    envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2
    started_event_sha256: str
    completed_event_sha256: str | None
    checkpoint_result: dict[str, object] | None


def _binding_payload(binding: GateCCheckpointLedgerBinding) -> dict[str, object]:
    if type(binding) is not GateCCheckpointLedgerBinding:
        raise _error("GATE_C_CHECKPOINT_BINDING_INVALID")
    rebuilt = GateCCheckpointLedgerBinding(
        campaign_identity=binding.campaign_identity,
        campaign_binding_sha256=binding.campaign_binding_sha256,
        scale_contract_sha256=binding.scale_contract_sha256,
        source_revision=binding.source_revision,
    )
    if rebuilt != binding:
        raise _error("GATE_C_CHECKPOINT_BINDING_INVALID")
    return {
        "schema_version": _BINDING_SCHEMA,
        "campaign_identity": rebuilt.campaign_identity,
        "campaign_binding_sha256": rebuilt.campaign_binding_sha256,
        "scale_contract_sha256": rebuilt.scale_contract_sha256,
        "source_revision": rebuilt.source_revision,
    }


def _binding_document(binding: GateCCheckpointLedgerBinding) -> dict[str, object]:
    payload = _binding_payload(binding)
    return {
        "binding_payload": payload,
        "binding_sha256": strict_canonical_digest(_BINDING_DOMAIN, payload),
    }


def _event_document(
    *,
    event_sequence: int,
    kind: GateCCheckpointEventKind,
    binding_sha256: str,
    envelope: GateCBoundedExecutionEnvelope,
    checkpoint_result: dict[str, object] | None,
    recorded_at_utc: str,
    previous_event_sha256: str,
) -> dict[str, object]:
    if type(event_sequence) is not int or event_sequence < 0:
        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
    if type(kind) is not GateCCheckpointEventKind:
        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
    _sha(binding_sha256)
    _sha(previous_event_sha256)
    _text(recorded_at_utc)
    envelope_document = gate_c_bounded_execution_envelope_document(envelope)
    if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED:
        if checkpoint_result is not None:
            raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
    else:
        if checkpoint_result is None:
            raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
        verify_gate_c_checkpoint_result(checkpoint_result, envelope=envelope)
    payload: dict[str, object] = {
        "schema_version": _EVENT_SCHEMA,
        "event_sequence": event_sequence,
        "event_kind": kind.value,
        "binding_sha256": binding_sha256,
        "envelope_sha256": envelope.envelope_sha256,
        "envelope": envelope_document if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED else None,
        "checkpoint_result": checkpoint_result,
        "recorded_at_utc": recorded_at_utc,
        "previous_event_sha256": previous_event_sha256,
    }
    return {
        "event_payload": payload,
        "event_sha256": strict_canonical_digest(_EVENT_DOMAIN, payload),
    }


def _event_document_v2(
    *,
    event_sequence: int,
    kind: GateCCheckpointEventKind,
    binding_sha256: str,
    envelope: GateCBoundedExecutionEnvelopeV2,
    checkpoint_result: dict[str, object] | None,
    recorded_at_utc: str,
    previous_event_sha256: str,
) -> dict[str, object]:
    if type(event_sequence) is not int or event_sequence < 0:
        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
    if type(kind) is not GateCCheckpointEventKind:
        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
    _sha(binding_sha256)
    _sha(previous_event_sha256)
    _text(recorded_at_utc)
    envelope_document = gate_c_bounded_execution_envelope_document_v2(envelope)
    if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED:
        if checkpoint_result is not None:
            raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
    else:
        if checkpoint_result is None:
            raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
        verify_gate_c_checkpoint_result_v2(checkpoint_result, envelope=envelope)
    payload: dict[str, object] = {
        "schema_version": _EVENT_SCHEMA_V2,
        "event_sequence": event_sequence,
        "event_kind": kind.value,
        "binding_sha256": binding_sha256,
        "envelope_sha256": envelope.envelope_sha256,
        "execution_source_revision": envelope.execution_source_revision,
        "envelope": envelope_document if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED else None,
        "checkpoint_result": checkpoint_result,
        "recorded_at_utc": recorded_at_utc,
        "previous_event_sha256": previous_event_sha256,
    }
    return {
        "event_payload": payload,
        "event_sha256": strict_canonical_digest(_EVENT_DOMAIN_V2, payload),
    }


def _event_document_for(
    *,
    event_sequence: int,
    kind: GateCCheckpointEventKind,
    binding_sha256: str,
    envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2,
    checkpoint_result: dict[str, object] | None,
    recorded_at_utc: str,
    previous_event_sha256: str,
) -> dict[str, object]:
    if type(envelope) is GateCBoundedExecutionEnvelope:
        return _event_document(
            event_sequence=event_sequence,
            kind=kind,
            binding_sha256=binding_sha256,
            envelope=envelope,
            checkpoint_result=checkpoint_result,
            recorded_at_utc=recorded_at_utc,
            previous_event_sha256=previous_event_sha256,
        )
    if type(envelope) is GateCBoundedExecutionEnvelopeV2:
        return _event_document_v2(
            event_sequence=event_sequence,
            kind=kind,
            binding_sha256=binding_sha256,
            envelope=envelope,
            checkpoint_result=checkpoint_result,
            recorded_at_utc=recorded_at_utc,
            previous_event_sha256=previous_event_sha256,
        )
    raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")


def _envelope_document_for(
    envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2,
) -> dict[str, object]:
    if type(envelope) is GateCBoundedExecutionEnvelope:
        return gate_c_bounded_execution_envelope_document(envelope)
    if type(envelope) is GateCBoundedExecutionEnvelopeV2:
        return gate_c_bounded_execution_envelope_document_v2(envelope)
    raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")


_TABLES = {
    "gate_c_checkpoint_binding": """
        CREATE TABLE gate_c_checkpoint_binding (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            binding_sha256 TEXT NOT NULL UNIQUE CHECK(length(binding_sha256)=64),
            document BLOB NOT NULL
        ) STRICT
    """.strip(),
    "gate_c_checkpoint_events": """
        CREATE TABLE gate_c_checkpoint_events (
            event_sequence INTEGER PRIMARY KEY CHECK(event_sequence>=0),
            event_kind TEXT NOT NULL CHECK(event_kind IN ('CHECKPOINT_STARTED','CHECKPOINT_COMPLETED')),
            envelope_sha256 TEXT NOT NULL CHECK(length(envelope_sha256)=64),
            document BLOB NOT NULL,
            previous_event_sha256 TEXT NOT NULL CHECK(length(previous_event_sha256)=64),
            event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64),
            UNIQUE(envelope_sha256,event_kind)
        ) STRICT
    """.strip(),
}
_TRIGGERS = {
    "gate_c_checkpoint_binding_no_update": "CREATE TRIGGER gate_c_checkpoint_binding_no_update BEFORE UPDATE ON gate_c_checkpoint_binding BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "gate_c_checkpoint_binding_no_delete": "CREATE TRIGGER gate_c_checkpoint_binding_no_delete BEFORE DELETE ON gate_c_checkpoint_binding BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "gate_c_checkpoint_events_no_update": "CREATE TRIGGER gate_c_checkpoint_events_no_update BEFORE UPDATE ON gate_c_checkpoint_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "gate_c_checkpoint_events_no_delete": "CREATE TRIGGER gate_c_checkpoint_events_no_delete BEFORE DELETE ON gate_c_checkpoint_events BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "gate_c_checkpoint_events_transition": """
        CREATE TRIGGER gate_c_checkpoint_events_transition
        BEFORE INSERT ON gate_c_checkpoint_events
        BEGIN
          SELECT CASE
            WHEN NEW.event_sequence != (SELECT COUNT(*) FROM gate_c_checkpoint_events)
              THEN RAISE(ABORT,'sequence')
            WHEN NEW.previous_event_sha256 != CASE
              WHEN NEW.event_sequence=0 THEN (SELECT binding_sha256 FROM gate_c_checkpoint_binding WHERE singleton=1)
              ELSE (SELECT event_sha256 FROM gate_c_checkpoint_events ORDER BY event_sequence DESC LIMIT 1)
            END THEN RAISE(ABORT,'chain')
            WHEN NEW.event_kind='CHECKPOINT_STARTED' AND EXISTS(
              SELECT 1 FROM gate_c_checkpoint_events AS started
              WHERE started.event_kind='CHECKPOINT_STARTED' AND NOT EXISTS(
                SELECT 1 FROM gate_c_checkpoint_events AS completed
                WHERE completed.envelope_sha256=started.envelope_sha256
                  AND completed.event_kind='CHECKPOINT_COMPLETED'
              )
            ) THEN RAISE(ABORT,'unfinished-checkpoint')
            WHEN NEW.event_kind='CHECKPOINT_COMPLETED' AND NOT EXISTS(
              SELECT 1 FROM gate_c_checkpoint_events
              WHERE envelope_sha256=NEW.envelope_sha256 AND event_kind='CHECKPOINT_STARTED'
            ) THEN RAISE(ABORT,'transition')
          END;
        END
    """.strip(),
}


class SQLiteGateCCheckpointLedger:
    """Process-owned append-only checkpoint audit ledger."""

    def __init__(self, path: Path, *, binding: GateCCheckpointLedgerBinding) -> None:
        self.path = Path(path)
        self.binding = binding
        self._mutex = threading.RLock()
        self._closed = False
        self._owner_pid = os.getpid()
        self._lock_fd = -1
        self._connection: sqlite3.Connection | None = None
        self._open()

    def _open(self) -> None:
        parent = self.path.parent
        info = parent.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o022
        ):
            raise _error("GATE_C_CHECKPOINT_PATH_UNSAFE")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        try:
            self._lock_fd = os.open(
                lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if self._lock_fd >= 0:
                os.close(self._lock_fd)
            self._lock_fd = -1
            raise _error("GATE_C_CHECKPOINT_LEDGER_OWNED") from exc
        lock_info = os.fstat(self._lock_fd)
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_uid != os.geteuid()
            or lock_info.st_nlink != 1
            or stat.S_IMODE(lock_info.st_mode) != 0o600
        ):
            self.close()
            raise _error("GATE_C_CHECKPOINT_PATH_UNSAFE")
        try:
            descriptor = os.open(
                self.path,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            created = False
        except OSError as exc:
            self.close()
            raise _error("GATE_C_CHECKPOINT_PATH_UNSAFE") from exc
        else:
            created = True
            os.close(descriptor)
        db_info = os.lstat(self.path)
        if (
            not stat.S_ISREG(db_info.st_mode)
            or db_info.st_uid != os.geteuid()
            or db_info.st_nlink != 1
            or stat.S_IMODE(db_info.st_mode) != 0o600
        ):
            self.close()
            raise _error("GATE_C_CHECKPOINT_PATH_UNSAFE")
        try:
            self._connection = sqlite3.connect(self.path)
            if created:
                os.chmod(self.path, 0o600)
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA trusted_schema=OFF")
            if created:
                self._create()
            self._verify_schema()
            self._verify_binding()
            self._states()
        except BaseException:
            self.close()
            raise

    @property
    def _db(self) -> sqlite3.Connection:
        if self._closed or self._connection is None:
            raise _error("GATE_C_CHECKPOINT_LEDGER_CLOSED")
        if os.getpid() != self._owner_pid:
            raise _error("GATE_C_CHECKPOINT_LEDGER_FORKED")
        return self._connection

    def _create(self) -> None:
        document = _binding_document(self.binding)
        db = self._db
        try:
            db.execute("BEGIN IMMEDIATE")
            for sql in _TABLES.values():
                db.execute(sql)
            for sql in _TRIGGERS.values():
                db.execute(sql)
            db.execute(
                "INSERT INTO gate_c_checkpoint_binding VALUES(1,?,?)",
                (
                    document["binding_sha256"],
                    strict_canonical_json_bytes(document),
                ),
            )
            db.execute("PRAGMA user_version=1")
            db.execute("COMMIT")
        except sqlite3.Error as exc:
            try:
                db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise _error("GATE_C_CHECKPOINT_LEDGER_CREATE_FAILED") from exc

    @staticmethod
    def _normalize_sql(value: str) -> str:
        return " ".join(value.strip().rstrip(";").split())

    def _verify_schema(self) -> None:
        db = self._db
        if db.execute("PRAGMA quick_check").fetchone() != ("ok",):
            raise _error("GATE_C_CHECKPOINT_LEDGER_INVALID")
        if db.execute("PRAGMA user_version").fetchone() != (1,):
            raise _error("GATE_C_CHECKPOINT_LEDGER_SCHEMA_INVALID")
        objects = db.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ).fetchall()
        expected = {
            **{("table", name): sql for name, sql in _TABLES.items()},
            **{("trigger", name): sql for name, sql in _TRIGGERS.items()},
        }
        actual = {(kind, name): sql for kind, name, sql in objects}
        if set(actual) != set(expected) or any(
            self._normalize_sql(actual[key]) != self._normalize_sql(sql)
            for key, sql in expected.items()
        ):
            raise _error("GATE_C_CHECKPOINT_LEDGER_SCHEMA_INVALID")

    def _verify_binding(self) -> str:
        row = self._db.execute(
            "SELECT binding_sha256,document FROM gate_c_checkpoint_binding WHERE singleton=1"
        ).fetchone()
        expected = _binding_document(self.binding)
        if row != (
            expected["binding_sha256"],
            strict_canonical_json_bytes(expected),
        ):
            raise _error("GATE_C_CHECKPOINT_BINDING_MISMATCH")
        return expected["binding_sha256"]

    def _states(self) -> tuple[GateCCheckpointLedgerState, ...]:
        binding_sha = self._verify_binding()
        previous = binding_sha
        chain_schema: str | None = None
        states: dict[str, GateCCheckpointLedgerState] = {}
        order: list[str] = []
        rows = self._db.execute(
            "SELECT event_sequence,event_kind,envelope_sha256,document,previous_event_sha256,event_sha256 FROM gate_c_checkpoint_events ORDER BY event_sequence"
        ).fetchall()
        for expected_sequence, row in enumerate(rows):
            try:
                document = json.loads(bytes(row[3]).decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID") from exc
            if strict_canonical_json_bytes(document) != bytes(row[3]):
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
            if type(document) is not dict or set(document) != {"event_payload", "event_sha256"}:
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
            payload = document["event_payload"]
            if type(payload) is not dict:
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
            schema_version = payload.get("schema_version")
            if type(schema_version) is not str:
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
            v1_fields = {
                "schema_version", "event_sequence", "event_kind", "binding_sha256",
                "envelope_sha256", "envelope", "checkpoint_result",
                "recorded_at_utc", "previous_event_sha256",
            }
            v2_fields = v1_fields | {"execution_source_revision"}
            if (
                schema_version == _EVENT_SCHEMA
                and set(payload) != v1_fields
            ) or (
                schema_version == _EVENT_SCHEMA_V2
                and set(payload) != v2_fields
            ) or schema_version not in {_EVENT_SCHEMA, _EVENT_SCHEMA_V2}:
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
            if chain_schema is None:
                chain_schema = schema_version
            elif schema_version != chain_schema:
                raise _error("GATE_C_CHECKPOINT_EVENT_VERSION_MIXED")
            try:
                kind = GateCCheckpointEventKind(payload["event_kind"])
            except (TypeError, ValueError) as exc:
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID") from exc
            envelope_sha = _sha(payload["envelope_sha256"])
            if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED:
                if any(
                    item.completed_event_sha256 is None for item in states.values()
                ):
                    raise _error("GATE_C_CHECKPOINT_TRANSITION_INVALID")
                if schema_version == _EVENT_SCHEMA:
                    envelope = parse_gate_c_bounded_execution_envelope_document(
                        payload["envelope"]
                    )
                else:
                    envelope = parse_gate_c_bounded_execution_envelope_document_v2(
                        payload["envelope"]
                    )
                    if payload["execution_source_revision"] != envelope.execution_source_revision:
                        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
                result = None
                if envelope.envelope_sha256 in states:
                    raise _error("GATE_C_CHECKPOINT_TRANSITION_INVALID")
                order.append(envelope.envelope_sha256)
            else:
                prior = states.get(envelope_sha)
                if prior is None or prior.completed_event_sha256 is not None:
                    raise _error("GATE_C_CHECKPOINT_TRANSITION_INVALID")
                envelope = prior.envelope
                if schema_version == _EVENT_SCHEMA:
                    if type(envelope) is not GateCBoundedExecutionEnvelope:
                        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
                    result = verify_gate_c_checkpoint_result(
                        payload["checkpoint_result"], envelope=envelope
                    )
                else:
                    if type(envelope) is not GateCBoundedExecutionEnvelopeV2:
                        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
                    if payload["execution_source_revision"] != envelope.execution_source_revision:
                        raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
                    result = verify_gate_c_checkpoint_result_v2(
                        payload["checkpoint_result"], envelope=envelope
                    )
            expected_document = _event_document_for(
                event_sequence=expected_sequence,
                kind=kind,
                binding_sha256=binding_sha,
                envelope=envelope,
                checkpoint_result=result,
                recorded_at_utc=payload["recorded_at_utc"],
                previous_event_sha256=previous,
            )
            if (
                row[0] != expected_sequence
                or row[1] != kind.value
                or row[2] != envelope.envelope_sha256
                or row[4] != previous
                or row[5] != expected_document["event_sha256"]
                or document != expected_document
            ):
                raise _error("GATE_C_CHECKPOINT_EVENT_INVALID")
            if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED:
                states[envelope_sha] = GateCCheckpointLedgerState(
                    envelope=envelope,
                    started_event_sha256=document["event_sha256"],
                    completed_event_sha256=None,
                    checkpoint_result=None,
                )
            else:
                prior = states[envelope_sha]
                states[envelope_sha] = GateCCheckpointLedgerState(
                    envelope=prior.envelope,
                    started_event_sha256=prior.started_event_sha256,
                    completed_event_sha256=document["event_sha256"],
                    checkpoint_result=result,
                )
            previous = document["event_sha256"]
        return tuple(states[item] for item in order)

    def state(
        self,
        envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2,
    ) -> GateCCheckpointLedgerState | None:
        document = _envelope_document_for(envelope)
        with self._mutex:
            return next(
                (item for item in self._states() if item.envelope.envelope_sha256 == document["envelope_sha256"]),
                None,
            )

    def _append(
        self,
        *,
        kind: GateCCheckpointEventKind,
        envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2,
        checkpoint_result: dict[str, object] | None,
        recorded_at_utc: str,
    ) -> GateCCheckpointLedgerState:
        with self._mutex:
            states = self._states()
            if states and any(type(item.envelope) is not type(envelope) for item in states):
                raise _error("GATE_C_CHECKPOINT_EVENT_VERSION_MIXED")
            current = next(
                (item for item in states if item.envelope.envelope_sha256 == envelope.envelope_sha256),
                None,
            )
            if kind is GateCCheckpointEventKind.CHECKPOINT_STARTED:
                if current is not None:
                    return current
                if any(item.completed_event_sha256 is None for item in states):
                    raise _error("GATE_C_CHECKPOINT_CONFLICT")
            elif current is None or current.completed_event_sha256 is not None:
                raise _error("GATE_C_CHECKPOINT_TRANSITION_INVALID")
            binding_sha = self._verify_binding()
            row = self._db.execute(
                "SELECT event_sha256 FROM gate_c_checkpoint_events ORDER BY event_sequence DESC LIMIT 1"
            ).fetchone()
            previous = binding_sha if row is None else row[0]
            document = _event_document_for(
                event_sequence=sum(2 if item.completed_event_sha256 else 1 for item in states),
                kind=kind,
                binding_sha256=binding_sha,
                envelope=envelope,
                checkpoint_result=checkpoint_result,
                recorded_at_utc=recorded_at_utc,
                previous_event_sha256=previous,
            )
            payload = document["event_payload"]
            try:
                self._db.execute("BEGIN IMMEDIATE")
                self._verify_schema()
                self._verify_binding()
                self._db.execute(
                    "INSERT INTO gate_c_checkpoint_events VALUES(?,?,?,?,?,?)",
                    (
                        payload["event_sequence"], kind.value,
                        envelope.envelope_sha256,
                        strict_canonical_json_bytes(document), previous,
                        document["event_sha256"],
                    ),
                )
                self._db.execute("COMMIT")
            except sqlite3.Error as exc:
                try:
                    self._db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("GATE_C_CHECKPOINT_DURABILITY_FAILED") from exc
            state = self.state(envelope)
            if state is None:
                raise _error("GATE_C_CHECKPOINT_RECONCILIATION_FAILED")
            return state

    def start(
        self,
        envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2,
        *,
        recorded_at_utc: str,
    ) -> GateCCheckpointLedgerState:
        return self._append(
            kind=GateCCheckpointEventKind.CHECKPOINT_STARTED,
            envelope=envelope,
            checkpoint_result=None,
            recorded_at_utc=recorded_at_utc,
        )

    def complete(
        self,
        envelope: GateCBoundedExecutionEnvelope | GateCBoundedExecutionEnvelopeV2,
        *,
        checkpoint_result: dict[str, object],
        recorded_at_utc: str,
    ) -> GateCCheckpointLedgerState:
        return self._append(
            kind=GateCCheckpointEventKind.CHECKPOINT_COMPLETED,
            envelope=envelope,
            checkpoint_result=checkpoint_result,
            recorded_at_utc=recorded_at_utc,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        owner_process = os.getpid() == self._owner_pid
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._lock_fd >= 0:
            try:
                # A forked child inherited the same open-file description;
                # explicit LOCK_UN here would release the parent's ownership.
                if owner_process:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)
                self._lock_fd = -1

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
