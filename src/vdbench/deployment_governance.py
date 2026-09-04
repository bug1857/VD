"""The single source-owned authority for where one deployment's governed state lives.

Purpose:
    ADR-022. Deployment-global governance state -- the canonical route-state
    marker and the verified-latest LKG (D2) authority store -- was addressed
    by caller-supplied filesystem path. Path selection therefore *defined*
    deployment identity, and independent review mechanically reproduced two
    P1 defects from that single root cause: one logical deployment could be
    given two LKG authority universes by naming two store paths (P1-A), and
    an unsafe ``ACTIVATING`` deployment could be made to look bootstrap-clean
    by naming a second, empty route-state path (P1-B). This module removes
    the degree of freedom rather than validating it: every deployment-global
    path is DERIVED here, from source-owned authority, and nothing outside
    this module recomputes an equivalent path.
Inputs:
    Nothing at production call sites. The canonical ENV-001 logical deployment
    identity is a source constant; the canonical governance root is DERIVED at
    call time from the OS account database (ADR-023), never from the process
    environment. ``resolve_deployment_governance_scope`` accepts an alternate
    root and identity for in-process test isolation only; no CLI flag,
    environment variable, operand, or config file reaches them.
Outputs:
    One immutable ``DeploymentGovernanceScope`` carrying the deployment
    identity, its namespace digest, and every derived deployment-global path.
Dependencies:
    ``canonical_serialization`` (v2 strict canonical JSON) and the standard
    library only. Deliberately imports no store, ledger, operator, or
    observation module, so every consumer can depend on this without a cycle.
Three identities, never collapsed (ADR-022 section 5):
    ``environment_identity`` (ADR-020 sections 5-7) is a RUNTIME-INSTANCE
    identity: it changes on container restart, restart count, started-at,
    image, collection schema, index identity and entity count, by design.
    Keying persistent governance on it would fork the namespace on the first
    Milvus restart and orphan both the route-state authority and the whole D2
    lineage. The LOGICAL DEPLOYMENT IDENTITY is stable across exactly those
    events. The DEPLOYMENT GOVERNANCE NAMESPACE is the filesystem namespace
    derived from it. This module owns only the third, and its namespace
    preimage therefore contains the logical deployment identity and nothing
    else -- no environment identity, no Milvus URI, no container identity, no
    configuration identity, no data identity, no ``source_run_id``. Those
    belong to other authority domains and binding them here would reintroduce
    the fork this module exists to prevent.
Complexity:
    Derivation is pure: one SHA-256 over a two-key document, then pure path
    composition. It performs no I/O beyond one OS account-database lookup and
    ``realpath`` on the root -- both reads -- so preflight and prepare can
    derive the complete scope while still creating nothing. Directory creation
    is a separate, explicit call.
Failure modes:
    Every refusal is a typed ``DeploymentGovernanceError`` with a stable code.
    An account home that cannot be resolved, or that is empty or relative, is
    refused with NO fallback to ``HOME``, ``expanduser``, XDG, the working
    directory or a temporary directory (ADR-023): inventing a root after a
    failed lookup would create exactly the second authority universe this
    module exists to prevent. An invalid deployment identity or an unsafe
    ``source_run_id`` component is refused before any path is built; a scope
    that exists as a symlink, a non-directory, or a group/world-accessible
    directory is refused rather than repaired, so no unrelated existing state
    is ever silently chmodded.
Extension points:
    A future ENV-002 requires its own explicit source and architecture
    addition (ADR-022 section 22); this module deliberately ships exactly one
    canonical identity and no registry, lookup table, or discovery mechanism.
"""

from __future__ import annotations

import hashlib
import os
import pwd
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from .canonical_serialization import strict_canonical_json_bytes

__all__ = [
    "CANONICAL_ENV001_DEPLOYMENT_IDENTITY",
    "CANONICAL_VD_GOVERNANCE_ROOT_SUFFIX",
    "DEPLOYMENT_NAMESPACE_DOMAIN",
    "DEPLOYMENT_NAMESPACE_SCHEMA_VERSION",
    "DEPLOYMENTS_DIRECTORY",
    "LKG_AUTHORITY_STORE_FILENAME",
    "ROUTE_STATE_FILENAME",
    "RUNS_DIRECTORY",
    "DeploymentGovernanceError",
    "DeploymentGovernanceScope",
    "canonical_account_home",
    "canonical_deployment_governance_scope",
    "canonical_vd_governance_root",
    "deployment_namespace_digest",
    "deployment_namespace_document",
    "ensure_deployment_scope_directory",
    "resolve_deployment_governance_scope",
]


#: The invariant source-owned suffix beneath the canonical account home
#: (ADR-022 section 6 as corrected by ADR-023). The POLICY is a source
#: constant; the account home it hangs from is resolved at CALL time from OS
#: authority, so no import-time snapshot of a process-environment value can be
#: taken. Changing this suffix is a source change that goes through normal
#: review, tests and source freeze.
CANONICAL_VD_GOVERNANCE_ROOT_SUFFIX = (".local", "share", "vd")

#: The human-assigned canonical logical deployment identity for the governed
#: ENV-001 deployment (ADR-022 section 5). It is a DERIVED/SOURCE FACT at every
#: production call site, never an operand: exposing it as a caller field would
#: replace one caller degree of freedom (the path) with another (the string),
#: which is exactly the defect under repair. A future ENV-002 requires its own
#: explicit source and architecture addition.
CANONICAL_ENV001_DEPLOYMENT_IDENTITY = "ENV-001-exp010-l2-v1"

DEPLOYMENT_NAMESPACE_SCHEMA_VERSION = "vd-deployment-governance-namespace-v1"

#: A fixed, versioned byte prefix -- never a JSON field. Disjoint from every
#: EXP-012 domain, from every ADR-020/ADR-020a identity domain, and from the
#: prepared-authority domain: a deployment namespace is not, and can never be
#: mistaken for, an environment, collection-schema, index, readiness, seal,
#: evaluation, or operator-authority identity.
DEPLOYMENT_NAMESPACE_DOMAIN = b"VD::DEPLOYMENT_GOVERNANCE_NAMESPACE::V1\x00"

DEPLOYMENTS_DIRECTORY = "deployments"
RUNS_DIRECTORY = "runs"
ROUTE_STATE_FILENAME = "route_state.json"
LKG_AUTHORITY_STORE_FILENAME = "lkg_authority.sqlite3"

#: The deployment identity becomes the digest preimage, and the digest becomes
#: one path component. The identity itself is constrained to the same strict,
#: path-safe charset already proven safe for ``source_run_id``, so the preimage
#: is canonical and no unicode, separator, or whitespace variant of one identity
#: can exist alongside it.
_DEPLOYMENT_IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")

#: A run root is one path component beneath ``runs``. This is a trust-boundary
#: guard on a public path-building API, not a restatement of the operator's
#: ``source_run_id`` operand contract: this module must never compose a
#: traversing or absolute path even if a caller reaches it with an unvalidated
#: value.
_PATH_COMPONENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class DeploymentGovernanceError(RuntimeError):
    """Fail-closed governance error carrying one stable reason code."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(code if message is None else f"{code}: {message}")
        self.code = code


def _error(code: str, message: str | None = None) -> DeploymentGovernanceError:
    return DeploymentGovernanceError(code, message)


def canonical_account_home() -> Path:
    """This OS account's home directory, from the account database alone.

    ADR-023. ``Path.home()`` and ``os.path.expanduser`` both resolve ``$HOME``
    on POSIX, so both let the process ENVIRONMENT select which governance
    universe is authoritative: same source bytes, same uid, same
    ``deployment_identity``, two values of ``HOME``, two route-state markers and
    two D2 lineages. That was reproduced mechanically and classified
    ``P1_HOME_SELECTABLE_GOVERNANCE_ROOT``. The account database is consulted
    instead, because it answers a uid, and a uid is not something an environment
    variable can spoof.

    ``os.getuid()`` -- the REAL uid -- is deliberate and narrow. Effective-uid,
    setuid-service and multi-user-daemon semantics are a different local-account
    authority contract; ADR-023 explicitly does not design them.

    Fails closed. There is NO fallback to ``HOME``, ``Path.home()``,
    ``expanduser``, ``XDG_DATA_HOME``, the working directory, or a temporary
    directory: a root invented after a failed lookup would be precisely the
    second authority universe this module exists to prevent.
    """

    try:
        record = pwd.getpwuid(os.getuid())
    except (KeyError, OSError, OverflowError, TypeError, ValueError) as exc:
        raise _error("DEPLOYMENT_ACCOUNT_HOME_UNRESOLVED", str(exc)) from exc
    home = getattr(record, "pw_dir", None)
    if type(home) is not str or not home:
        raise _error(
            "DEPLOYMENT_ACCOUNT_HOME_INVALID", "pw_dir must be a non-empty string"
        )
    if not os.path.isabs(home):
        # A relative account home would be joined with the working directory,
        # making governance authority depend on where the process started.
        raise _error("DEPLOYMENT_ACCOUNT_HOME_INVALID", "pw_dir must be absolute")
    return Path(home)


def canonical_vd_governance_root() -> Path:
    """The one canonical production governance root, derived at call time.

    ``<account home>/.local/share/vd``. Source authority for the SUFFIX, OS
    account authority for the HOME: not a CLI flag, not an operand, not an
    environment variable, not ``HOME``, not ``XDG_DATA_HOME``, not a config
    file, not ``campaign_root``, not the Milvus URI, not the working directory,
    and not a mutable module global production can rebind. Changing the suffix
    is a governed source change; changing the account home requires actually
    being a different OS account, which is a different local-account authority
    boundary rather than a way to fork one account's governance state.

    Deliberately a function and not an import-time constant (ADR-023): the
    POLICY is constant, but the account-database RESULT is a runtime OS fact.
    An import-time snapshot could neither fail closed on a bad account record
    nor be exercised without reimporting the module.
    """

    return canonical_account_home().joinpath(*CANONICAL_VD_GOVERNANCE_ROOT_SUFFIX)


def deployment_namespace_document(deployment_identity: str) -> dict[str, str]:
    """The exact, closed namespace preimage for one logical deployment.

    Two keys, and deliberately only two. Every other identity the system
    carries -- environment, endpoint, container, configuration, data, index,
    run -- belongs to a different authority domain and is excluded, because a
    namespace that moved when any of them moved would fork the deployment's
    persistent governance state on an ordinary container restart.
    """

    if type(deployment_identity) is not str or (
        _DEPLOYMENT_IDENTITY_RE.fullmatch(deployment_identity) is None
    ):
        raise _error(
            "DEPLOYMENT_IDENTITY_INVALID",
            "deployment_identity must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}",
        )
    return {
        "schema_version": DEPLOYMENT_NAMESPACE_SCHEMA_VERSION,
        "deployment_identity": deployment_identity,
    }


def deployment_namespace_digest(deployment_identity: str) -> str:
    """``sha256(domain + strict_canonical_json_bytes(document))``, 64 lowercase hex.

    Untruncated, matching every other identity digest in this repository. A
    digest rather than the raw identity becomes the path component because a
    raw-identity directory would collide under case folding on a
    case-insensitive filesystem and would admit unicode spelling variance.
    """

    document = deployment_namespace_document(deployment_identity)
    return hashlib.sha256(
        DEPLOYMENT_NAMESPACE_DOMAIN + strict_canonical_json_bytes(document)
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class DeploymentGovernanceScope:
    """The one canonical governance scope for one logical deployment.

    Every deployment-global path this system owns is a field here or derives
    from ``runs_root`` through :meth:`run_root`. Production code receives this
    object; it never recomputes an equivalent path of its own, so there is
    exactly one place a deployment's route-state authority and its
    verified-latest LKG authority can be.
    """

    deployment_identity: str
    namespace_digest: str
    canonical_root: str
    scope_root: str
    route_state_path: str
    lkg_authority_store_path: str
    runs_root: str

    def run_root(self, source_run_id: str) -> str:
        """The one canonical run root for ``source_run_id`` in this scope."""

        if type(source_run_id) is not str or (
            _PATH_COMPONENT_RE.fullmatch(source_run_id) is None
        ):
            raise _error(
                "DEPLOYMENT_RUN_ID_INVALID",
                "source_run_id must be one safe path component",
            )
        return str(Path(self.runs_root) / source_run_id)


def resolve_deployment_governance_scope(
    *,
    deployment_identity: str = CANONICAL_ENV001_DEPLOYMENT_IDENTITY,
    canonical_root: str | os.PathLike[str] | None = None,
) -> DeploymentGovernanceScope:
    """Derive one deployment's complete governance scope. Creates nothing.

    Pure apart from one OS account-database lookup and ``realpath`` on the
    root, both of which are reads: preflight and prepare can therefore derive
    and print the entire scope while still creating no file of any kind.
    Anchoring on ``realpath`` before any child path is composed is what removes
    the lexical-alias variance the previous ``Path(store_path).parent``
    derivation carried.

    ``canonical_root`` defaults to ``None``, meaning the canonical
    OS-account-derived root; an explicit value exists for in-process test
    isolation only. No CLI flag, environment variable, operand, or config file
    reaches either parameter, so no production caller can select a different
    scope. ``expanduser`` is deliberately NOT applied to an injected root
    either: a ``~`` reaching this function would resolve ``$HOME`` and
    reintroduce exactly the process-environment authority ADR-023 removes.
    """

    digest = deployment_namespace_digest(deployment_identity)
    if canonical_root is None:
        candidate = os.fspath(canonical_vd_governance_root())
    else:
        candidate = os.fspath(canonical_root)
    # Checked BEFORE ``realpath``, which would otherwise silently join a
    # relative root with the current working directory and make the whole
    # governance scope depend on where the process happened to start.
    if not os.path.isabs(candidate):
        raise _error("DEPLOYMENT_ROOT_INVALID", "canonical root must be absolute")
    root = Path(os.path.realpath(candidate))
    scope_root = root / DEPLOYMENTS_DIRECTORY / digest
    return DeploymentGovernanceScope(
        deployment_identity=deployment_identity,
        namespace_digest=digest,
        canonical_root=str(root),
        scope_root=str(scope_root),
        route_state_path=str(scope_root / ROUTE_STATE_FILENAME),
        lkg_authority_store_path=str(scope_root / LKG_AUTHORITY_STORE_FILENAME),
        runs_root=str(scope_root / RUNS_DIRECTORY),
    )


def canonical_deployment_governance_scope() -> DeploymentGovernanceScope:
    """The production scope: OS-account-derived root, canonical ENV-001 identity.

    The only scope any production call site can obtain. Both of its inputs are
    authority this process cannot choose: the account database answers the uid,
    and the deployment identity is a source constant.
    """

    return resolve_deployment_governance_scope()


def ensure_deployment_scope_directory(scope: DeploymentGovernanceScope) -> None:
    """Create the scope directory private, or refuse an unsafe existing one.

    Deliberately separate from derivation so that no read-only mode can create
    state by merely resolving a path. An existing scope that is a symlink, a
    non-directory, or group/world-accessible is REFUSED rather than repaired:
    silently chmodding or replacing pre-existing state on a shared host would
    be a data-loss hazard, and this stays inside ADR-020 section 44's
    cooperative, non-hostile single-host threat model.
    """

    path = Path(scope.scope_root)
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        entry = os.lstat(path)
    except OSError as exc:
        raise _error("DEPLOYMENT_SCOPE_UNUSABLE", str(exc)) from exc
    if not stat.S_ISDIR(entry.st_mode):
        # ``lstat`` deliberately does not follow: a symlink standing where the
        # scope belongs is an alias, and an alias is never the authority.
        raise _error(
            "DEPLOYMENT_SCOPE_NOT_A_DIRECTORY", scope.scope_root
        )
    mode = stat.S_IMODE(entry.st_mode)
    if mode & 0o077:
        raise _error("DEPLOYMENT_SCOPE_NOT_PRIVATE", oct(mode))
