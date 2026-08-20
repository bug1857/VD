"""Durable marker that mechanically separates EXP-012 from EXP-010."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from .canonical_serialization import strict_canonical_digest, strict_canonical_json_bytes
from .exp012_scale_contract import (
    Exp012ScaleContract,
    Exp012ScaleProfile,
    build_exp012_scale_contract,
    exp012_scale_contract_payload,
    verify_exp012_scale_contract,
)

EXP012_SCALE_CAMPAIGN_MARKER = "exp012_scale_campaign.json"
_SCHEMA = "exp012-scale-campaign-v1"
_DOMAIN = b"VD::EXP012_SCALE_CAMPAIGN::V1\x00"


class Exp012ScaleCampaignError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _error(code: str) -> Exp012ScaleCampaignError:
    return Exp012ScaleCampaignError(code)


@dataclass(frozen=True, slots=True)
class Exp012ScaleCampaignBinding:
    contract: Exp012ScaleContract
    gate_a_evidence_sha256: str
    campaign_sha256: str


def _sha(value: object) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(item not in "0123456789abcdef" for item in value)
    ):
        raise _error("EXP012_SCALE_GATE_A_DIGEST_INVALID")
    return value


def _document(
    contract: Exp012ScaleContract, gate_a_evidence_sha256: str
) -> dict[str, object]:
    verified = verify_exp012_scale_contract(contract)
    gate_a_digest = _sha(gate_a_evidence_sha256)
    payload: dict[str, object] = {
        "schema_version": _SCHEMA,
        "experiment_id": "EXP-012-SCALE",
        "scale_contract": exp012_scale_contract_payload(verified),
        "scale_contract_sha256": verified.contract_sha256,
        "gate_a_evidence_sha256": gate_a_digest,
    }
    return {
        "campaign_payload": payload,
        "campaign_sha256": strict_canonical_digest(_DOMAIN, payload),
    }


def marker_path(campaign_root: Path) -> Path:
    return Path(campaign_root) / EXP012_SCALE_CAMPAIGN_MARKER


def write_scale_campaign_marker(
    campaign_root: Path,
    contract: Exp012ScaleContract,
    *,
    gate_a_evidence_sha256: str,
) -> Path:
    """Create the marker exactly once, or verify an identical existing marker."""

    path = marker_path(campaign_root)
    expected = strict_canonical_json_bytes(
        _document(contract, gate_a_evidence_sha256)
    )
    if path.exists():
        load_scale_campaign_marker(
            campaign_root,
            expected_contract=contract,
            expected_gate_a_evidence_sha256=gate_a_evidence_sha256,
        )
        return path
    parent = path.parent
    if not parent.exists():
        container = parent.parent
        container_info = container.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(container_info.st_mode)
            or container_info.st_uid != os.geteuid()
            or stat.S_IMODE(container_info.st_mode) & 0o022
        ):
            raise _error("EXP012_SCALE_CAMPAIGN_PARENT_UNSAFE")
        try:
            parent.mkdir(mode=0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise _error("EXP012_SCALE_CAMPAIGN_WRITE_FAILED") from exc
    info = parent.stat(follow_symlinks=False)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o022
    ):
        raise _error("EXP012_SCALE_CAMPAIGN_PARENT_UNSAFE")
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(expected)
            handle.flush()
            os.fsync(handle.fileno())
        directory_fd = os.open(
            parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except FileExistsError:
        load_scale_campaign_marker(
            campaign_root,
            expected_contract=contract,
            expected_gate_a_evidence_sha256=gate_a_evidence_sha256,
        )
    except OSError as exc:
        raise _error("EXP012_SCALE_CAMPAIGN_WRITE_FAILED") from exc
    return path


def load_scale_campaign_marker(
    campaign_root: Path,
    *,
    expected_contract: Exp012ScaleContract | None = None,
    expected_gate_a_evidence_sha256: str | None = None,
) -> Exp012ScaleCampaignBinding:
    path = marker_path(campaign_root)
    try:
        info = os.lstat(path)
        raw = path.read_bytes()
    except OSError as exc:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_MISSING") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_UNSAFE")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_INVALID") from exc
    if strict_canonical_json_bytes(document) != raw:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_INVALID")
    if type(document) is not dict or set(document) != {"campaign_payload", "campaign_sha256"}:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_INVALID")
    payload = document["campaign_payload"]
    if type(payload) is not dict or set(payload) != {
        "schema_version", "experiment_id", "scale_contract",
        "scale_contract_sha256", "gate_a_evidence_sha256",
    }:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_INVALID")
    try:
        profile = Exp012ScaleProfile(payload["scale_contract"]["profile"])
        contract = build_exp012_scale_contract(profile)
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_INVALID") from exc
    gate_a_digest = _sha(payload["gate_a_evidence_sha256"])
    expected_document = _document(contract, gate_a_digest)
    if payload != expected_document["campaign_payload"] or document != expected_document:
        raise _error("EXP012_SCALE_CAMPAIGN_MARKER_INVALID")
    if expected_contract is not None and contract != verify_exp012_scale_contract(
        expected_contract
    ):
        raise _error("EXP012_SCALE_CAMPAIGN_CONTRACT_MISMATCH")
    if (
        expected_gate_a_evidence_sha256 is not None
        and gate_a_digest != _sha(expected_gate_a_evidence_sha256)
    ):
        raise _error("EXP012_SCALE_GATE_A_AUTHORITY_MISMATCH")
    return Exp012ScaleCampaignBinding(
        contract=contract,
        gate_a_evidence_sha256=gate_a_digest,
        campaign_sha256=str(document["campaign_sha256"]),
    )
