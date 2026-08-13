"""ADR-014 durable attestation + previous-window-evidence store.

Purpose:
    Persist one `RealDetectorAttestation` and the encoded `WindowEvidence` that
    produced it, keyed by the exact detector head digest, so that (a) a head can
    later be proven real and (b) the next adjacent comparison under the same
    reference can *fetch* its previous evidence instead of being handed one.
Codec reuse:
    `WindowEvidence` is encoded with the existing
    `monitor_evidence.encode_persisted_window_evidence` and restored with
    `decode_persisted_window_evidence`; no second codec exists.
Ordering:
    Heads persist first (ADR-012 detector store), attestations second. A crash
    between the two leaves a head with no attestation, which is simply not
    real-eligible -- the safe direction.
Hardening:
    Private parent/path, `O_NOFOLLOW`, exclusive flock plus in-process inode
    ownership, STRICT tables, `synchronous=FULL`, `BEGIN IMMEDIATE`,
    append-only triggers, exact-set schema verification, canonical
    serialization, a hash chain, and full restart re-verification. No automatic
    repair, migration, overwrite, or delete exists.
Authority:
    None. No policy, grant, routing, admission, activation, actuation, or
    candidate authority; no Milvus or network dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading

from .artifacts import canonical_json_bytes
from .drift import DetectorState, DriftClassification, WindowEvidence
from .host_window_detector_v2 import (
    SQLiteHostWindowDetectorV2Store,
    V2DetectorHead,
    VerifiedLatestV2DetectorHead,
)
from .monitor_evidence import (
    decode_persisted_window_evidence,
    encode_persisted_window_evidence,
)
from .real_detector_attestation import (
    ATTESTATION_SCHEMA_VERSION,
    PreviousAttestedEvidence,
    RealDetectorAttestation,
    attestation_document,
)
from .shadow_event_types import MonitorStreamKey


__all__ = [
    "RealDetectorAttestationStoreError",
    "VerifiedRealDetectorHead",
    "SQLiteRealDetectorAttestationStore",
]


_DB_VERSION = 1
_RECORD_DOMAIN = b"VD::REAL_DETECTOR_ATTESTATION_RECORD::V1\x00"
_BINDING_DOMAIN = b"VD::REAL_DETECTOR_ATTESTATION_STORE::V1\x00"

_OWNED_LOCK_INODES: set[tuple[int, int]] = set()
_OWNERSHIP_LOCK = threading.Lock()

_ISSUE_TOKEN = object()


class RealDetectorAttestationStoreError(RuntimeError):
    """Fail-closed store error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


def _error(code: str, message: str | None = None) -> RealDetectorAttestationStoreError:
    return RealDetectorAttestationStoreError(code, message)


def _digest(domain: bytes, payload: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class VerifiedRealDetectorHead:
    """A store-issued head proven to come from the governed real detector."""

    head: V2DetectorHead
    attestation: RealDetectorAttestation
    head_record_sequence: int
    head_record_sha256: str
    head_record_persisted_at_utc: str

    def __init__(self, *, _token: object, **values: object) -> None:
        if _token is not _ISSUE_TOKEN:
            raise TypeError("verified real detector heads are store-issued")
        for name, value in values.items():
            object.__setattr__(self, name, value)


_SCHEMA_SQL = (
    "CREATE TABLE attestation_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding_json BLOB NOT NULL, binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64)) STRICT",
    "CREATE TABLE attestation_records (record_sequence INTEGER PRIMARY KEY CHECK(record_sequence>=0), detector_head_sha256 TEXT NOT NULL UNIQUE CHECK(length(detector_head_sha256)=64), current_window_sequence INTEGER NOT NULL UNIQUE CHECK(current_window_sequence>=0), record_json BLOB NOT NULL, previous_record_sha256 TEXT, record_sha256 TEXT NOT NULL UNIQUE CHECK(length(record_sha256)=64)) STRICT",
    "CREATE TRIGGER attestation_binding_no_update BEFORE UPDATE ON attestation_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attestation_binding_no_delete BEFORE DELETE ON attestation_binding BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attestation_records_no_update BEFORE UPDATE ON attestation_records BEGIN SELECT RAISE(ABORT,'append-only'); END",
    "CREATE TRIGGER attestation_records_no_delete BEFORE DELETE ON attestation_records BEGIN SELECT RAISE(ABORT,'append-only'); END",
)


def _normalize_sql(value: str) -> str:
    return " ".join(value.split())


class SQLiteRealDetectorAttestationStore:
    """Exclusive-writer, append-only attestation and previous-evidence store."""

    def __init__(
        self, path: str | os.PathLike[str], *, stream_key: MonitorStreamKey
    ) -> None:
        from .host_window_detector_v2 import _stream_document

        if type(stream_key) is not MonitorStreamKey:
            raise _error("ATTESTATION_STREAM_INVALID")
        self.path = Path(path)
        self.stream_key = stream_key
        self._binding = {
            "schema_version": ATTESTATION_SCHEMA_VERSION,
            "stream": _stream_document(stream_key),
        }
        self._mutex = threading.RLock()
        self._connection: sqlite3.Connection | None = None
        self._lock_handle = None
        self._lock_inode: tuple[int, int] | None = None
        self._closed = False
        self._open()

    def __enter__(self) -> "SQLiteRealDetectorAttestationStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _open(self) -> None:
        parent = self.path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_info = parent.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(parent_info.st_mode)
            or parent_info.st_uid != os.geteuid()
            or stat.S_IMODE(parent_info.st_mode) & 0o022
        ):
            raise _error("ATTESTATION_PARENT_UNSAFE")
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open(lock_path, flags, 0o600)
        self._lock_handle = os.fdopen(lock_fd, "a+b")
        info = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
        ):
            self.close()
            raise _error("ATTESTATION_PATH_UNSAFE")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock_handle.close()
            self._lock_handle = None
            raise _error("ATTESTATION_STORE_BUSY") from exc
        inode = (info.st_dev, info.st_ino)
        with _OWNERSHIP_LOCK:
            if inode in _OWNED_LOCK_INODES:
                self.close()
                raise _error("ATTESTATION_STORE_BUSY")
            _OWNED_LOCK_INODES.add(inode)
        self._lock_inode = inode
        created = not self.path.exists()
        if created:
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.close(fd)
        self._verify_path()
        self._connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        try:
            if created:
                self._connection.execute("PRAGMA journal_mode=DELETE")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
                self._connection.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA_SQL:
                    self._connection.execute(statement)
                self._connection.execute(f"PRAGMA user_version={_DB_VERSION}")
                self._connection.execute(
                    "INSERT INTO attestation_binding VALUES(1,?,?)",
                    (
                        canonical_json_bytes(self._binding),
                        _digest(_BINDING_DOMAIN, self._binding),
                    ),
                )
                self._connection.execute("COMMIT")
            else:
                mode = str(self._connection.execute("PRAGMA journal_mode").fetchone()[0])
                if mode.lower() != "delete":
                    raise _error("ATTESTATION_SCHEMA_INVALID")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.execute("PRAGMA trusted_schema=OFF")
            self._verify_all()
        except Exception:
            self.close()
            raise

    def _verify_path(self) -> None:
        info = os.lstat(self.path)
        if (
            not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise _error("ATTESTATION_PATH_UNSAFE")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._connection is not None:
            try:
                self._connection.close()
            except sqlite3.Error:
                pass
            self._connection = None
        if self._lock_inode is not None:
            with _OWNERSHIP_LOCK:
                _OWNED_LOCK_INODES.discard(self._lock_inode)
            self._lock_inode = None
        if self._lock_handle is not None:
            try:
                self._lock_handle.close()
            except OSError:
                pass
            self._lock_handle = None

    def _require_live(self) -> sqlite3.Connection:
        if self._closed or self._connection is None:
            raise _error("ATTESTATION_STORE_CLOSED")
        return self._connection

    def _verify_schema(self) -> None:
        connection = self._require_live()
        if connection.execute("PRAGMA user_version").fetchone()[0] != _DB_VERSION:
            raise _error("ATTESTATION_SCHEMA_INVALID")
        actual = {
            row[0]: _normalize_sql(row[1])
            for row in connection.execute(
                "SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected = {
            statement.split()[2]: _normalize_sql(statement) for statement in _SCHEMA_SQL
        }
        if actual != expected:
            raise _error("ATTESTATION_SCHEMA_INVALID")

    def _verify_binding(self) -> None:
        connection = self._require_live()
        row = connection.execute(
            "SELECT binding_json,binding_sha256 FROM attestation_binding WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise _error("ATTESTATION_BINDING_MISSING")
        document = json.loads(bytes(row[0]).decode("utf-8"))
        if document != self._binding or row[1] != _digest(_BINDING_DOMAIN, self._binding):
            raise _error("ATTESTATION_BINDING_MISMATCH")

    def _verify_all(self) -> tuple[dict[str, object], ...]:
        self._verify_schema()
        self._verify_binding()
        connection = self._require_live()
        rows = connection.execute(
            "SELECT record_sequence,detector_head_sha256,current_window_sequence,record_json,previous_record_sha256,record_sha256 FROM attestation_records ORDER BY record_sequence"
        ).fetchall()
        previous: str | None = None
        documents: list[dict[str, object]] = []
        for index, row in enumerate(rows):
            if row[0] != index or row[4] != previous:
                raise _error("ATTESTATION_CHAIN_INVALID")
            document = json.loads(bytes(row[3]).decode("utf-8"))
            if (
                type(document) is not dict
                or document.get("detector_head_sha256") != row[1]
                or document.get("current_window_sequence") != row[2]
            ):
                raise _error("ATTESTATION_RECORD_INVALID")
            if _digest(_RECORD_DOMAIN, document) != row[5]:
                raise _error("ATTESTATION_RECORD_DIGEST_MISMATCH")
            documents.append(document)
            previous = row[5]
        return tuple(documents)

    # -- append ----------------------------------------------------------

    def append(
        self,
        *,
        attestation: RealDetectorAttestation,
        window_evidence: WindowEvidence,
    ) -> str:
        """Durably record one attestation and its current window evidence.

        Returns the encoded evidence digest so the caller can confirm the
        attestation binds exactly what was stored.
        """

        with self._mutex:
            if type(attestation) is not RealDetectorAttestation:
                raise _error("ATTESTATION_INVALID")
            if attestation.stream_key != self.stream_key:
                raise _error("ATTESTATION_STREAM_MISMATCH")
            document_attestation = attestation_document(attestation)
            encoded_evidence = encode_persisted_window_evidence(window_evidence)
            evidence_digest = encoded_evidence["sha256"]
            if attestation.current_window_evidence_sha256 != evidence_digest:
                raise _error("ATTESTATION_EVIDENCE_MISMATCH")
            existing = self._verify_all()
            record = {
                "schema_version": ATTESTATION_SCHEMA_VERSION,
                "detector_head_sha256": attestation.detector_head_sha256,
                "current_window_sequence": attestation.current_window_sequence,
                "attestation": document_attestation,
                "window_evidence": encoded_evidence,
            }
            previous = None
            if existing:
                previous = _digest(_RECORD_DOMAIN, existing[-1])
            digest = _digest(_RECORD_DOMAIN, record)
            connection = self._require_live()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO attestation_records VALUES(?,?,?,?,?,?)",
                    (
                        len(existing), attestation.detector_head_sha256,
                        attestation.current_window_sequence,
                        canonical_json_bytes(record), previous, digest,
                    ),
                )
                connection.execute("COMMIT")
            except sqlite3.Error as exc:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise _error("ATTESTATION_APPEND_FAILED") from exc
            return evidence_digest

    # -- previous-evidence fetch port ------------------------------------

    def load_previous_attested_evidence(
        self,
        *,
        stream_key: object,
        reference_window_sequence: int,
        reference_source_window_sha256: str,
        expected_current_window_sequence: int,
        detector_contract_identity: str,
    ) -> PreviousAttestedEvidence | None:
        """Fetch the immediate predecessor comparison under the same reference.

        Returns None -- never a weaker fallback -- when the predecessor does not
        exist, belongs to a different reference epoch, is non-adjacent, or was
        produced under a different detector contract.
        """

        with self._mutex:
            if stream_key != self.stream_key:
                raise _error("ATTESTATION_STREAM_MISMATCH")
            if expected_current_window_sequence < 0:
                return None
            documents = self._verify_all()
            for document in documents:
                if document["current_window_sequence"] != expected_current_window_sequence:
                    continue
                payload = document["attestation"]["attestation_payload"]
                if (
                    payload["reference_window_sequence"] != reference_window_sequence
                    or payload["reference_source_window_sha256"]
                    != reference_source_window_sha256
                    or payload["detector_contract_identity"] != detector_contract_identity
                ):
                    return None
                try:
                    evidence = decode_persisted_window_evidence(
                        document["window_evidence"]
                    )
                except Exception as exc:  # codec raises its own fail-closed error
                    raise _error("ATTESTATION_EVIDENCE_UNDECODABLE") from exc
                if document["window_evidence"]["sha256"] != payload["current_window_evidence_sha256"]:
                    raise _error("ATTESTATION_EVIDENCE_MISMATCH")
                return PreviousAttestedEvidence(
                    attestation_sha256=document["attestation"]["attestation_sha256"],
                    window_evidence_sha256=payload["current_window_evidence_sha256"],
                    window_evidence=evidence,
                    current_window_sequence=payload["current_window_sequence"],
                    reference_window_sequence=payload["reference_window_sequence"],
                    reference_source_window_sha256=payload["reference_source_window_sha256"],
                    detector_contract_identity=payload["detector_contract_identity"],
                    detector_head_sha256=payload["detector_head_sha256"],
                )
            return None

    # -- real head verification ------------------------------------------

    def load_verified_real_latest(
        self, detector_store: SQLiteHostWindowDetectorV2Store
    ) -> VerifiedRealDetectorHead | None:
        """Issue a real head only when head and attestation bind each other.

        Inherits ADR-012's gap/rebaseline semantics unchanged: if the detector
        store reports no latest head (after WINDOW_UNEVALUABLE or REBASELINE),
        this returns None. A head with no attestation is never real evidence.
        """

        with self._mutex:
            if type(detector_store) is not SQLiteHostWindowDetectorV2Store:
                raise _error("ATTESTATION_DETECTOR_STORE_INVALID")
            latest = detector_store.load_verified_latest(self.stream_key)
            if latest is None or type(latest) is not VerifiedLatestV2DetectorHead:
                return None
            head = latest.head
            documents = self._verify_all()
            match = None
            for document in documents:
                if document["detector_head_sha256"] == head.detector_head_sha256:
                    match = document
                    break
            if match is None:
                return None
            payload = match["attestation"]["attestation_payload"]
            from .host_window_detector_v2 import _stream_document

            if (
                payload["stream"] != _stream_document(head.stream_key)
                or payload["reference_window_sequence"] != head.reference_window_sequence
                or payload["reference_source_window_sha256"]
                != head.reference_source_window_sha256
                or payload["current_window_sequence"] != head.current_window_sequence
                or payload["current_source_window_sha256"]
                != head.current_source_window_sha256
                or payload["current_shadow_window_sha256"]
                != head.current_shadow_window_sha256
                or payload["detector_state"] != head.detector_state.value
                or payload["detector_classification"] != head.detector_classification.value
                or payload["evidence_provenance_sha256"] != head.detector_provenance.sha256
            ):
                raise _error("ATTESTATION_HEAD_MISMATCH")
            attestation = self._attestation_from_document(match["attestation"])
            return VerifiedRealDetectorHead(
                _token=_ISSUE_TOKEN,
                head=head,
                attestation=attestation,
                head_record_sequence=latest.head_record_sequence,
                head_record_sha256=latest.head_record_sha256,
                head_record_persisted_at_utc=latest.head_record_persisted_at_utc,
            )

    def _attestation_from_document(self, document: dict[str, object]) -> RealDetectorAttestation:
        """Rebuild one attestation value; the digest is re-verified, and the
        private token is used only after that verification succeeds."""

        payload = document["attestation_payload"]
        if document["attestation_sha256"] != _digest(
            b"VD::REAL_DETECTOR_ATTESTATION::V1\x00", payload
        ):
            raise _error("ATTESTATION_DIGEST_MISMATCH")
        from .real_detector_attestation import _ISSUE_TOKEN as _ATTESTATION_TOKEN

        return RealDetectorAttestation(
            _token=_ATTESTATION_TOKEN,
            schema_version=ATTESTATION_SCHEMA_VERSION,
            detector_contract_identity=payload["detector_contract_identity"],
            stream_key=self.stream_key,
            reference_window_sequence=payload["reference_window_sequence"],
            reference_source_window_sha256=payload["reference_source_window_sha256"],
            reference_shadow_window_sha256=payload["reference_shadow_window_sha256"],
            reference_assembled_manifest_sha256=payload["reference_assembled_manifest_sha256"],
            current_window_sequence=payload["current_window_sequence"],
            current_source_window_sha256=payload["current_source_window_sha256"],
            current_shadow_window_sha256=payload["current_shadow_window_sha256"],
            current_assembled_manifest_sha256=payload["current_assembled_manifest_sha256"],
            detector_seed=payload["detector_seed"],
            previous_window_evidence_sha256=payload["previous_window_evidence_sha256"],
            previous_attestation_sha256=payload["previous_attestation_sha256"],
            current_window_evidence_sha256=payload["current_window_evidence_sha256"],
            evidence_provenance_sha256=payload["evidence_provenance_sha256"],
            detector_state=DetectorState(payload["detector_state"]),
            detector_classification=DriftClassification(payload["detector_classification"]),
            source_revision=payload["source_revision"],
            environment_manifest_sha256=payload["environment_manifest_sha256"],
            detector_head_sha256=payload["detector_head_sha256"],
            attestation_sha256=document["attestation_sha256"],
        )
