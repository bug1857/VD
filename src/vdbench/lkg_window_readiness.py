"""Pre-seal operational-readiness capture for one LKG-qualification window.

Purpose:
    Give Checkpoint B (ARCHITECTURE.md's ADR-002 Phase-2 addendum,
    ``73ade90``) the typed, pre-seal half of its readiness contract:
    ``LkgWindowOperationalReadinessEvidence`` -- one immutable record of
    the health + rollback-readiness check for exactly one 200-position
    constituent window -- and the provider contract that produces it
    (``LkgWindowOperationalReadinessProvider``, with
    ``FakeLkgWindowOperationalReadinessProvider`` the only implementation
    this checkpoint ships). This module never reads a ledger, never
    performs I/O, and never binds to a sealed run -- that is
    ``lkg_phase2_readiness_ledger.py``'s job.
Chronology:
    This evidence is captured at window-completion time, strictly before
    the enclosing qualification run can be sealed -- its evidence
    therefore has no field referencing a seal, because none exists yet at
    that moment. Binding this evidence to a specific seal, after the fact,
    is ``lkg_phase2_source_binding.py``'s ``LkgWindowReadinessIngestion``.
Capture vs. lookup:
    ``capture_or_return`` is the pre-seal, window-completion-time entry
    point: an unseen ``readiness_check_id`` performs (or, for a fake
    provider, simulates) the one logical check for that window and stores
    it durably; a retry with the same ``readiness_check_id`` returns the
    stored result unchanged, never re-executing. ``lookup`` is the
    post-seal, Phase-2-ingestion-time entry point: it retrieves an
    already-captured result by ``readiness_check_id`` alone and MUST NEVER
    execute a new check. One ``(source_run_id, window_index)`` pair maps
    to exactly one ``readiness_check_id``, forever -- a window may be
    captured exactly once, enforced by the provider itself at capture
    time, not only later by ingestion.
Failure modes:
    Every validation failure in this module raises ``ContractViolation``.
    Provider-specific capture/lookup failures raise
    ``LkgWindowOperationalReadinessProviderError`` with one of
    ``RESULT_NOT_RECOVERABLE``, ``READINESS_CHECK_ID_CONFLICTING_RESULT``,
    ``READINESS_WINDOW_ALREADY_CHECKED``.
Orchestration precondition (caller-owned, not mechanically enforced here):
    ``capture_or_return(...)`` is valid only after the corresponding
    200-query Phase-1 window has completed its intended durable attempt
    traversal, and before the full run is sealed. This module deliberately
    never reads a ledger (see ``Purpose`` above), so it cannot and does not
    verify either half of that precondition -- neither that the window's
    200 positions actually completed, nor that the run is not yet sealed.
    Both are the calling orchestration's responsibility. Consequently,
    readiness evidence produced by this module is never, by itself,
    sufficient proof that a window's Phase-1 positions completed --
    Checkpoint C's window/epoch evaluation MUST independently and
    mechanically verify Phase-1 window completeness/classification from
    the sealed ledger before letting readiness evidence contribute to a
    PASSING/FAILING verdict, rather than inferring completeness from the
    mere presence of readiness evidence.
"""

from __future__ import annotations

import hashlib
import re
import threading
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Callable, Protocol

from .artifacts import canonical_json_bytes
from .config import ContractViolation


__all__ = [
    "READINESS_SCHEMA_VERSION",
    "READINESS_DOMAIN",
    "validate_rfc3339_utc",
    "parse_rfc3339_utc_instant",
    "LkgWindowOperationalReadinessProviderError",
    "LkgWindowOperationalReadinessEvidence",
    "readiness_payload_document",
    "readiness_payload_document_digest",
    "lkg_window_operational_readiness_evidence_from_payload",
    "LkgWindowOperationalReadinessProvider",
    "FakeLkgWindowOperationalReadinessProvider",
]


READINESS_SCHEMA_VERSION = 1
READINESS_DOMAIN = b"vdbench.lkg_window_operational_readiness.v1\0"

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RFC3339_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z"
)
_REASON_CODE_RE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_MAX_READINESS_REASON_CODES = 16
_MAX_TEXT_CODEPOINTS = 256
_WINDOWS_PER_RUN = 12
_EPOCHS_PER_RUN = 2
_WINDOWS_PER_EPOCH = 6
_POSITIONS_PER_WINDOW = 200


def _canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ContractViolation(f"{field} must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if (
        not normalized
        or normalized != value
        or value.strip() != value
        or len(value) > _MAX_TEXT_CODEPOINTS
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ContractViolation(f"{field} is not canonical")
    return value


def _sha256_hex(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be a lowercase 64-character hex SHA-256 digest")
    return value


def _nonnegative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractViolation(f"{field} must be a non-negative integer")
    return value


def _bool(value: object, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise ContractViolation(f"{field} must be a bool")
    return value


def validate_rfc3339_utc(value: object, *, field: str) -> str:
    """Checkpoint-B's canonical RFC3339-UTC validator: UTC ``Z`` suffix,
    optional 1-6 fractional digits, a syntactically valid instant.
    Independently implemented from, but contract-identical to,
    ``lkg_qualification_seal.py``'s module-private ``_rfc3339_utc`` --
    that validator is not exported, so this checkpoint defines its own
    rather than reaching across the module-privacy boundary."""

    if not isinstance(value, str) or _RFC3339_UTC_RE.fullmatch(value) is None:
        raise ContractViolation(f"{field} must be RFC3339 UTC ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ContractViolation(f"{field} must be a valid RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ContractViolation(f"{field} must use UTC")
    return value


def parse_rfc3339_utc_instant(value: str) -> datetime:
    """Parse an already-canonical RFC3339-UTC string (i.e. one that has
    already passed ``validate_rfc3339_utc``) into a comparable instant.
    Callers requiring ordering comparisons MUST use this -- never raw
    string comparison, which is unsound across differing fractional-digit
    precision."""

    return datetime.fromisoformat(value[:-1] + "+00:00")


class LkgWindowOperationalReadinessProviderError(RuntimeError):
    """A provider-specific capture/lookup failure: ``RESULT_NOT_RECOVERABLE``,
    ``READINESS_CHECK_ID_CONFLICTING_RESULT``, or
    ``READINESS_WINDOW_ALREADY_CHECKED``."""


@dataclass(frozen=True, slots=True)
class LkgWindowOperationalReadinessEvidence:
    """One immutable, pre-seal record of the health + rollback-readiness
    check for exactly one 200-position constituent window.

    Every invariant is enforced in ``__post_init__`` on any construction
    path, direct or reconstructed, ending with an independent
    recompute-and-compare of ``canonical_document_digest`` itself --
    stronger than Checkpoint A's ``LkgRunSeal``, which validates digest
    *format* only in ``__post_init__``.
    """

    readiness_schema_version: int
    source_run_id: str
    source_run_binding_sha256: str
    window_index: int
    epoch_index: int
    first_attempt_sequence: int
    last_attempt_sequence: int
    readiness_check_id: str
    provider_run_id: str
    health_checked: bool
    health_passed: bool
    health_evidence_source_identity: str
    health_evidence_source_digest: str
    rollback_tested: bool
    rollback_ready: bool
    rollback_evidence_source_identity: str
    rollback_evidence_source_digest: str
    checked_at_utc: str
    check_start_ns: int
    check_end_ns: int
    reason_codes: tuple[str, ...]
    canonical_document_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "readiness_schema_version",
            _nonnegative_int(self.readiness_schema_version, field="readiness_schema_version"),
        )
        if self.readiness_schema_version != READINESS_SCHEMA_VERSION:
            raise ContractViolation(f"readiness_schema_version must equal {READINESS_SCHEMA_VERSION}")
        object.__setattr__(self, "source_run_id", _canonical_text(self.source_run_id, field="source_run_id"))
        object.__setattr__(
            self,
            "source_run_binding_sha256",
            _sha256_hex(self.source_run_binding_sha256, field="source_run_binding_sha256"),
        )
        object.__setattr__(
            self, "window_index", _nonnegative_int(self.window_index, field="window_index")
        )
        if not 0 <= self.window_index < _WINDOWS_PER_RUN:
            raise ContractViolation(f"window_index must be in [0, {_WINDOWS_PER_RUN})")
        object.__setattr__(
            self, "epoch_index", _nonnegative_int(self.epoch_index, field="epoch_index")
        )
        if not 0 <= self.epoch_index < _EPOCHS_PER_RUN or self.epoch_index != self.window_index // _WINDOWS_PER_EPOCH:
            raise ContractViolation("epoch_index must equal window_index // 6 and be in [0, 2)")
        object.__setattr__(
            self,
            "first_attempt_sequence",
            _nonnegative_int(self.first_attempt_sequence, field="first_attempt_sequence"),
        )
        if self.first_attempt_sequence != self.window_index * _POSITIONS_PER_WINDOW:
            raise ContractViolation("first_attempt_sequence must equal window_index * 200")
        object.__setattr__(
            self,
            "last_attempt_sequence",
            _nonnegative_int(self.last_attempt_sequence, field="last_attempt_sequence"),
        )
        if self.last_attempt_sequence != self.first_attempt_sequence + _POSITIONS_PER_WINDOW - 1:
            raise ContractViolation("last_attempt_sequence must equal first_attempt_sequence + 199")

        object.__setattr__(
            self, "readiness_check_id", _canonical_text(self.readiness_check_id, field="readiness_check_id")
        )
        object.__setattr__(
            self, "provider_run_id", _canonical_text(self.provider_run_id, field="provider_run_id")
        )

        object.__setattr__(self, "health_checked", _bool(self.health_checked, field="health_checked"))
        object.__setattr__(self, "health_passed", _bool(self.health_passed, field="health_passed"))
        if self.health_passed and not self.health_checked:
            raise ContractViolation("health_passed cannot be true when health_checked is false")
        object.__setattr__(
            self,
            "health_evidence_source_identity",
            _canonical_text(self.health_evidence_source_identity, field="health_evidence_source_identity"),
        )
        object.__setattr__(
            self,
            "health_evidence_source_digest",
            _sha256_hex(self.health_evidence_source_digest, field="health_evidence_source_digest"),
        )

        object.__setattr__(self, "rollback_tested", _bool(self.rollback_tested, field="rollback_tested"))
        object.__setattr__(self, "rollback_ready", _bool(self.rollback_ready, field="rollback_ready"))
        if self.rollback_ready and not self.rollback_tested:
            raise ContractViolation("rollback_ready cannot be true when rollback_tested is false")
        object.__setattr__(
            self,
            "rollback_evidence_source_identity",
            _canonical_text(
                self.rollback_evidence_source_identity, field="rollback_evidence_source_identity"
            ),
        )
        object.__setattr__(
            self,
            "rollback_evidence_source_digest",
            _sha256_hex(self.rollback_evidence_source_digest, field="rollback_evidence_source_digest"),
        )

        object.__setattr__(
            self, "checked_at_utc", validate_rfc3339_utc(self.checked_at_utc, field="checked_at_utc")
        )
        object.__setattr__(
            self, "check_start_ns", _nonnegative_int(self.check_start_ns, field="check_start_ns")
        )
        object.__setattr__(self, "check_end_ns", _nonnegative_int(self.check_end_ns, field="check_end_ns"))
        if self.check_end_ns < self.check_start_ns:
            raise ContractViolation("check_end_ns must be >= check_start_ns")

        if not isinstance(self.reason_codes, tuple) or not all(
            isinstance(code, str) for code in self.reason_codes
        ):
            raise ContractViolation("reason_codes must be a tuple of strings")
        if len(self.reason_codes) > _MAX_READINESS_REASON_CODES:
            raise ContractViolation(f"reason_codes must contain at most {_MAX_READINESS_REASON_CODES} entries")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ContractViolation("reason_codes must not contain duplicates")
        if tuple(sorted(self.reason_codes)) != self.reason_codes:
            raise ContractViolation("reason_codes must be supplied in lexicographically sorted order")
        for code in self.reason_codes:
            if _REASON_CODE_RE.fullmatch(code) is None:
                raise ContractViolation(f"reason_codes entry {code!r} is not a canonical reason code")

        object.__setattr__(
            self,
            "canonical_document_digest",
            _sha256_hex(self.canonical_document_digest, field="canonical_document_digest"),
        )

        # Direct-construction digest self-verification (never merely a
        # format check): rebuild the payload from this object's own,
        # now-fully-validated fields and require the digest matches.
        recomputed_payload = readiness_payload_document(self)
        recomputed_digest = readiness_payload_document_digest(recomputed_payload)
        if recomputed_digest != self.canonical_document_digest:
            raise ContractViolation(
                "canonical_document_digest does not match the recomputed payload digest"
            )


_READINESS_PAYLOAD_FIELDS = frozenset(
    {
        "readiness_schema_version",
        "source_run_id",
        "source_run_binding_sha256",
        "window_index",
        "epoch_index",
        "first_attempt_sequence",
        "last_attempt_sequence",
        "readiness_check_id",
        "provider_run_id",
        "health_checked",
        "health_passed",
        "health_evidence_source_identity",
        "health_evidence_source_digest",
        "rollback_tested",
        "rollback_ready",
        "rollback_evidence_source_identity",
        "rollback_evidence_source_digest",
        "checked_at_utc",
        "check_start_ns",
        "check_end_ns",
        "reason_codes",
    }
)


def readiness_payload_document(evidence: LkgWindowOperationalReadinessEvidence) -> dict[str, object]:
    """The canonical, hashed payload -- deliberately excludes
    ``canonical_document_digest`` itself."""

    if not isinstance(evidence, LkgWindowOperationalReadinessEvidence):
        raise ContractViolation("evidence must be an LkgWindowOperationalReadinessEvidence")
    return {
        "readiness_schema_version": evidence.readiness_schema_version,
        "source_run_id": evidence.source_run_id,
        "source_run_binding_sha256": evidence.source_run_binding_sha256,
        "window_index": evidence.window_index,
        "epoch_index": evidence.epoch_index,
        "first_attempt_sequence": evidence.first_attempt_sequence,
        "last_attempt_sequence": evidence.last_attempt_sequence,
        "readiness_check_id": evidence.readiness_check_id,
        "provider_run_id": evidence.provider_run_id,
        "health_checked": evidence.health_checked,
        "health_passed": evidence.health_passed,
        "health_evidence_source_identity": evidence.health_evidence_source_identity,
        "health_evidence_source_digest": evidence.health_evidence_source_digest,
        "rollback_tested": evidence.rollback_tested,
        "rollback_ready": evidence.rollback_ready,
        "rollback_evidence_source_identity": evidence.rollback_evidence_source_identity,
        "rollback_evidence_source_digest": evidence.rollback_evidence_source_digest,
        "checked_at_utc": evidence.checked_at_utc,
        "check_start_ns": evidence.check_start_ns,
        "check_end_ns": evidence.check_end_ns,
        "reason_codes": list(evidence.reason_codes),
    }


def readiness_payload_document_digest(payload_document: dict[str, object]) -> str:
    return hashlib.sha256(READINESS_DOMAIN + canonical_json_bytes(payload_document)).hexdigest()


def lkg_window_operational_readiness_evidence_from_payload(
    payload_document: object, *, canonical_document_digest: str
) -> LkgWindowOperationalReadinessEvidence:
    """Strictly reconstruct evidence from its canonical payload plus a
    digest supplied separately by the caller -- the payload itself
    carries no digest field to trust instead."""

    if not isinstance(payload_document, dict) or set(payload_document) != _READINESS_PAYLOAD_FIELDS:
        raise ContractViolation("readiness payload document must contain exactly the expected fields")
    reason_codes_value = payload_document["reason_codes"]
    if not isinstance(reason_codes_value, list) or not all(
        isinstance(code, str) for code in reason_codes_value
    ):
        raise ContractViolation("reason_codes must be a list of strings")
    return LkgWindowOperationalReadinessEvidence(
        readiness_schema_version=payload_document["readiness_schema_version"],
        source_run_id=payload_document["source_run_id"],
        source_run_binding_sha256=payload_document["source_run_binding_sha256"],
        window_index=payload_document["window_index"],
        epoch_index=payload_document["epoch_index"],
        first_attempt_sequence=payload_document["first_attempt_sequence"],
        last_attempt_sequence=payload_document["last_attempt_sequence"],
        readiness_check_id=payload_document["readiness_check_id"],
        provider_run_id=payload_document["provider_run_id"],
        health_checked=payload_document["health_checked"],
        health_passed=payload_document["health_passed"],
        health_evidence_source_identity=payload_document["health_evidence_source_identity"],
        health_evidence_source_digest=payload_document["health_evidence_source_digest"],
        rollback_tested=payload_document["rollback_tested"],
        rollback_ready=payload_document["rollback_ready"],
        rollback_evidence_source_identity=payload_document["rollback_evidence_source_identity"],
        rollback_evidence_source_digest=payload_document["rollback_evidence_source_digest"],
        checked_at_utc=payload_document["checked_at_utc"],
        check_start_ns=payload_document["check_start_ns"],
        check_end_ns=payload_document["check_end_ns"],
        reason_codes=tuple(reason_codes_value),
        canonical_document_digest=canonical_document_digest,
    )


class LkgWindowOperationalReadinessProvider(Protocol):
    """Pre-seal capture / post-seal lookup contract for one window's
    operational-readiness check. See the module docstring for the
    capture-vs-lookup distinction."""

    def capture_or_return(
        self,
        *,
        readiness_check_id: str,
        source_run_id: str,
        source_run_binding_sha256: str,
        window_index: int,
        epoch_index: int,
        first_attempt_sequence: int,
        last_attempt_sequence: int,
    ) -> LkgWindowOperationalReadinessEvidence:
        """Pre-seal, window-completion-time path.

        An unseen ``readiness_check_id`` performs (or, for a fake
        provider, simulates) the ONE logical health+rollback check for
        this window, generating a fresh ``provider_run_id``/
        ``checked_at_utc``/``check_start_ns``/``check_end_ns``, and
        durably stores it under two keys: ``readiness_check_id`` ->
        evidence, and ``(source_run_id, window_index)`` ->
        ``readiness_check_id``.

        A previously-seen ``readiness_check_id`` whose context
        (``source_run_id``, ``window_index``, ``epoch_index``,
        ``first_attempt_sequence``, ``last_attempt_sequence``,
        ``source_run_binding_sha256``) still agrees returns the stored
        evidence unchanged -- never re-executes. A previously-seen
        ``readiness_check_id`` whose context now disagrees raises
        ``LkgWindowOperationalReadinessProviderError("READINESS_CHECK_ID_CONFLICTING_RESULT")``.
        A new ``readiness_check_id`` for a ``(source_run_id,
        window_index)`` that already has a *different*
        ``readiness_check_id`` on record raises
        ``LkgWindowOperationalReadinessProviderError("READINESS_WINDOW_ALREADY_CHECKED")``
        -- a window may be captured exactly once, ever, enforced here at
        capture time.

        Orchestration precondition: valid only after the corresponding
        200-query Phase-1 window has completed its intended durable
        attempt traversal, and before the full run is sealed. This
        provider does not inspect Phase-1 completeness -- see the module
        docstring's ``Orchestration precondition`` section. Do not treat
        the returned evidence as proof that the window completed.
        """
        ...

    def lookup(self, *, readiness_check_id: str) -> LkgWindowOperationalReadinessEvidence:
        """Post-seal, Phase-2-ingestion-time path. MUST NEVER execute a
        new check -- retrieves only historically captured evidence for an
        already-known ``readiness_check_id``. Takes no window/run context
        arguments; the stored evidence already carries all of that.
        ``provider_run_id`` in the returned evidence is provider-owned
        provenance from the ORIGINAL ``capture_or_return`` execution --
        never regenerated here. Raises
        ``LkgWindowOperationalReadinessProviderError("RESULT_NOT_RECOVERABLE")``
        if the check_id is unknown or its evidence cannot be retrieved.
        """
        ...


def _default_readiness_builder(
    *,
    readiness_check_id: str,
    provider_run_id: str,
    source_run_id: str,
    source_run_binding_sha256: str,
    window_index: int,
    epoch_index: int,
    first_attempt_sequence: int,
    last_attempt_sequence: int,
) -> LkgWindowOperationalReadinessEvidence:
    """The default, deterministic "clean pass" builder used by
    ``FakeLkgWindowOperationalReadinessProvider`` when no custom builder
    is supplied -- no live Milvus/etcd/MinIO/rollback-drill code."""

    checked_at_utc = "2026-01-01T00:00:00.000000Z"
    payload = {
        "readiness_schema_version": READINESS_SCHEMA_VERSION,
        "source_run_id": source_run_id,
        "source_run_binding_sha256": source_run_binding_sha256,
        "window_index": window_index,
        "epoch_index": epoch_index,
        "first_attempt_sequence": first_attempt_sequence,
        "last_attempt_sequence": last_attempt_sequence,
        "readiness_check_id": readiness_check_id,
        "provider_run_id": provider_run_id,
        "health_checked": True,
        "health_passed": True,
        "health_evidence_source_identity": "FakeLkgWindowOperationalReadinessProvider",
        "health_evidence_source_digest": "a" * 64,
        "rollback_tested": True,
        "rollback_ready": True,
        "rollback_evidence_source_identity": "FakeLkgWindowOperationalReadinessProvider",
        "rollback_evidence_source_digest": "b" * 64,
        "checked_at_utc": checked_at_utc,
        "check_start_ns": 0,
        "check_end_ns": 1,
        "reason_codes": [],
    }
    digest = readiness_payload_document_digest(payload)
    return lkg_window_operational_readiness_evidence_from_payload(payload, canonical_document_digest=digest)


_ReadinessBuilder = Callable[..., LkgWindowOperationalReadinessEvidence]


class FakeLkgWindowOperationalReadinessProvider:
    """Deterministic, test-only provider. Maintains its own idempotency
    store (``readiness_check_id`` -> evidence, and ``(source_run_id,
    window_index)`` -> ``readiness_check_id``) under one lock, so
    ``capture_or_return`` genuinely enforces exactly-once-per-window
    execution even under real concurrency -- the configured builder is
    called at most once per distinct ``readiness_check_id``, while still
    holding the lock, so no interleaving can invoke it twice for the same
    check. No live Milvus/etcd/MinIO/rollback-drill code exists anywhere
    in this class.
    """

    def __init__(self, *, builder: _ReadinessBuilder = _default_readiness_builder) -> None:
        self._lock = threading.Lock()
        self._builder = builder
        self._by_check_id: dict[str, LkgWindowOperationalReadinessEvidence] = {}
        self._window_to_check_id: dict[tuple[str, int], str] = {}
        self._poisoned: set[str] = set()

    def capture_or_return(
        self,
        *,
        readiness_check_id: str,
        source_run_id: str,
        source_run_binding_sha256: str,
        window_index: int,
        epoch_index: int,
        first_attempt_sequence: int,
        last_attempt_sequence: int,
    ) -> LkgWindowOperationalReadinessEvidence:
        context = (
            source_run_id,
            window_index,
            epoch_index,
            first_attempt_sequence,
            last_attempt_sequence,
            source_run_binding_sha256,
        )
        with self._lock:
            existing = self._by_check_id.get(readiness_check_id)
            if existing is not None:
                existing_context = (
                    existing.source_run_id,
                    existing.window_index,
                    existing.epoch_index,
                    existing.first_attempt_sequence,
                    existing.last_attempt_sequence,
                    existing.source_run_binding_sha256,
                )
                if existing_context != context:
                    raise LkgWindowOperationalReadinessProviderError(
                        "READINESS_CHECK_ID_CONFLICTING_RESULT"
                    )
                return existing

            existing_check_id_for_window = self._window_to_check_id.get((source_run_id, window_index))
            if existing_check_id_for_window is not None and existing_check_id_for_window != readiness_check_id:
                raise LkgWindowOperationalReadinessProviderError("READINESS_WINDOW_ALREADY_CHECKED")

            evidence = self._builder(
                readiness_check_id=readiness_check_id,
                provider_run_id=f"provider-run-{readiness_check_id}",
                source_run_id=source_run_id,
                source_run_binding_sha256=source_run_binding_sha256,
                window_index=window_index,
                epoch_index=epoch_index,
                first_attempt_sequence=first_attempt_sequence,
                last_attempt_sequence=last_attempt_sequence,
            )
            self._by_check_id[readiness_check_id] = evidence
            self._window_to_check_id[(source_run_id, window_index)] = readiness_check_id
            return evidence

    def lookup(self, *, readiness_check_id: str) -> LkgWindowOperationalReadinessEvidence:
        with self._lock:
            if readiness_check_id in self._poisoned or readiness_check_id not in self._by_check_id:
                raise LkgWindowOperationalReadinessProviderError("RESULT_NOT_RECOVERABLE")
            return self._by_check_id[readiness_check_id]

    def poison(self, readiness_check_id: str) -> None:
        """Test helper: simulate provider-side loss of an already-known
        check_id's historical result -- subsequent ``lookup`` calls raise
        ``RESULT_NOT_RECOVERABLE`` without ever invoking the builder."""

        with self._lock:
            self._poisoned.add(readiness_check_id)
