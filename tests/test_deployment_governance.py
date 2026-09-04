"""Focused tests for the canonical deployment governance scope (ADR-022, ADR-023).

These prove the P1-A/P1-B closure at its root: exactly one logical deployment
maps to exactly one governance scope, that scope's two deployment-global
authority paths are derived rather than chosen, and nothing a production caller
can express selects a different one.

ADR-023 adds the account-identity half of that guarantee. The scope is now
invariant under the process environment as well as under the operand set, so
``HOME`` -- which ``Path.home()`` and ``expanduser`` both honour -- can no
longer select a second governance universe for one OS account.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import pwd
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from vdbench.deployment_governance import (
    CANONICAL_ENV001_DEPLOYMENT_IDENTITY,
    CANONICAL_VD_GOVERNANCE_ROOT_SUFFIX,
    DEPLOYMENT_NAMESPACE_DOMAIN,
    DEPLOYMENT_NAMESPACE_SCHEMA_VERSION,
    DEPLOYMENTS_DIRECTORY,
    LKG_AUTHORITY_STORE_FILENAME,
    ROUTE_STATE_FILENAME,
    RUNS_DIRECTORY,
    DeploymentGovernanceError,
    DeploymentGovernanceScope,
    canonical_account_home,
    canonical_deployment_governance_scope,
    canonical_vd_governance_root,
    deployment_namespace_digest,
    deployment_namespace_document,
    ensure_deployment_scope_directory,
    resolve_deployment_governance_scope,
)

_RUN_ID = "lkg-qualification-run-0001"


def _executable_source(source: str) -> str:
    """The module's code with every docstring removed.

    A forbidden-token scan over raw text cannot distinguish ``Path.home()``
    being CALLED from a docstring explaining why it is never called. This
    reparses and unparses with docstring expression statements dropped, so the
    assertion is about behaviour rather than about prose.
    """

    tree = ast.parse(source)
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(ast.fix_missing_locations(tree))


class _TempRootCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name) / "vd-governance"

    def scope(self, **kwargs):
        return resolve_deployment_governance_scope(canonical_root=self.root, **kwargs)


# ======================================================================
# 1-4. Source authority: root, identity, schema, domain
# ======================================================================


class SourceAuthorityTests(unittest.TestCase):
    def test_canonical_root_suffix_is_the_source_constant(self) -> None:
        """Only the SUFFIX is a constant; the account home is resolved at call time.

        ADR-023: an import-time ``Path.home()``-derived constant is exactly what
        made the root environment-selectable, so the shape itself is part of the
        contract. No user-specific absolute literal is committed.
        """

        self.assertEqual(CANONICAL_VD_GOVERNANCE_ROOT_SUFFIX, (".local", "share", "vd"))

    def test_canonical_root_is_the_account_home_plus_the_suffix(self) -> None:
        """Expected value computed by an INDEPENDENT test-side account lookup."""

        expected = Path(pwd.getpwuid(os.getuid()).pw_dir).joinpath(
            ".local", "share", "vd"
        )
        self.assertEqual(canonical_vd_governance_root(), expected)
        self.assertTrue(canonical_vd_governance_root().is_absolute())
        self.assertEqual(canonical_account_home(), Path(pwd.getpwuid(os.getuid()).pw_dir))

    def test_canonical_env001_deployment_identity_is_exact(self) -> None:
        self.assertEqual(CANONICAL_ENV001_DEPLOYMENT_IDENTITY, "ENV-001-exp010-l2-v1")

    def test_namespace_schema_version_is_exact(self) -> None:
        self.assertEqual(
            DEPLOYMENT_NAMESPACE_SCHEMA_VERSION, "vd-deployment-governance-namespace-v1"
        )

    def test_namespace_domain_is_exact(self) -> None:
        self.assertEqual(
            DEPLOYMENT_NAMESPACE_DOMAIN, b"VD::DEPLOYMENT_GOVERNANCE_NAMESPACE::V1\x00"
        )

    def test_no_environment_override_exists_in_the_module_source(self) -> None:
        """No env var, XDG lookup, or CLI hook can move the production root.

        ADR-023 adds ``Path.home`` and ``expanduser`` to the forbidden set:
        both silently resolve ``$HOME``, which is exactly what made the root
        environment-selectable. They are checked against EXECUTABLE code only,
        because this module's docstrings deliberately name them to record why
        they are refused.
        """

        source = Path("src/vdbench/deployment_governance.py").read_text()
        # Raw-text scan for tokens the module has no reason to name at all.
        # ``XDG_`` and ``Path.home`` moved to the executable-only scan below,
        # because ADR-023's rationale names both in prose.
        for forbidden in ("os.environ", "getenv", "argparse", "sys.argv", "input("):
            self.assertNotIn(forbidden, source, forbidden)

        executable = _executable_source(source)
        for forbidden in ("Path.home", "expanduser", "environ", "getenv", "XDG"):
            self.assertNotIn(forbidden, executable, f"{forbidden} in executable code")
        # The account database IS the authority, and it is actually reached.
        self.assertIn("pwd.getpwuid", executable)
        self.assertIn("os.getuid", executable)


# ======================================================================
# 5-10. The namespace digest, and what it deliberately does NOT bind
# ======================================================================


class NamespaceDigestTests(unittest.TestCase):
    def _independent_digest(self, identity: str) -> str:
        """Hand-built vector: literal domain, literal document, stdlib only.

        Deliberately does not call ``deployment_namespace_document`` or
        ``deployment_namespace_digest`` -- an expected value produced by the
        code under test proves nothing.
        """

        document = {
            "schema_version": "vd-deployment-governance-namespace-v1",
            "deployment_identity": identity,
        }
        encoded = (
            json.dumps(
                document,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        return hashlib.sha256(
            b"VD::DEPLOYMENT_GOVERNANCE_NAMESPACE::V1\x00" + encoded
        ).hexdigest()

    def test_independent_vector_reproduces_the_production_digest(self) -> None:
        for identity in (CANONICAL_ENV001_DEPLOYMENT_IDENTITY, "ENV-002-probe", "a"):
            with self.subTest(identity=identity):
                self.assertEqual(
                    deployment_namespace_digest(identity),
                    self._independent_digest(identity),
                )

    def test_digest_is_sixty_four_lowercase_hex_untruncated(self) -> None:
        digest = deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY)
        self.assertEqual(len(digest), 64)
        self.assertTrue(all(character in "0123456789abcdef" for character in digest))

    def test_derivation_is_deterministic(self) -> None:
        digests = {
            deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY)
            for _ in range(8)
        }
        self.assertEqual(len(digests), 1)

    def test_preimage_is_exactly_two_keys(self) -> None:
        document = deployment_namespace_document(CANONICAL_ENV001_DEPLOYMENT_IDENTITY)
        self.assertEqual(
            set(document), {"schema_version", "deployment_identity"}
        )

    def test_no_environment_identity_or_endpoint_is_in_the_preimage(self) -> None:
        """The namespace must not move when a runtime-instance fact moves.

        ``environment_identity`` (ADR-020 sections 5-7) changes on container
        restart, restart count, started-at, image, collection schema, index
        identity and entity count -- by design. Binding any of it here would
        fork this deployment's persistent governance state, orphaning its
        route-state authority and its whole D2 lineage, on the first restart.
        """

        rendered = json.dumps(
            deployment_namespace_document(CANONICAL_ENV001_DEPLOYMENT_IDENTITY)
        )
        for foreign in (
            "lkg-env-identity",
            "environment_identity",
            "milvus",
            "19530",
            "localhost",
            "127.0.0.1",
            "container",
            "collection",
            "index_identity",
            "entity_count",
            "source_run_id",
            "configuration_identity",
            "data_identity",
        ):
            self.assertNotIn(foreign, rendered, foreign)

    def test_simulated_runtime_change_cannot_change_the_namespace(self) -> None:
        """Restart, image, index rebuild and data change are not inputs at all.

        The strongest form of the guarantee: those facts have no parameter to
        vary, so the namespace is invariant under every one of them by
        construction rather than by comparison.
        """

        baseline = deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY)
        for _ in range(4):
            self.assertEqual(
                deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY),
                baseline,
            )
        self.assertEqual(
            deployment_namespace_document.__code__.co_argcount,
            1,
            "one input: the logical deployment identity",
        )

    def test_invalid_deployment_identity_is_refused(self) -> None:
        for hostile in (
            "",
            ".",
            "..",
            ".hidden",
            "a/b",
            "/abs",
            "a\\b",
            "a\x00b",
            "a b",
            "a:b",
            "rün-01",
            "a" * 129,
            " lead",
            "trail ",
        ):
            with self.subTest(identity=hostile):
                with self.assertRaises(DeploymentGovernanceError) as caught:
                    deployment_namespace_digest(hostile)
                self.assertEqual(caught.exception.code, "DEPLOYMENT_IDENTITY_INVALID")


# ======================================================================
# 11, 16-20. The scope and every path derived from it
# ======================================================================


class ScopeDerivationTests(_TempRootCase):
    def test_scope_root_is_root_deployments_digest(self) -> None:
        scope = self.scope()
        expected = (
            Path(os.path.realpath(self.root))
            / DEPLOYMENTS_DIRECTORY
            / deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY)
        )
        self.assertEqual(scope.scope_root, str(expected))
        self.assertEqual(scope.canonical_root, os.path.realpath(self.root))

    def test_route_state_path_is_exact(self) -> None:
        scope = self.scope()
        self.assertEqual(
            scope.route_state_path, str(Path(scope.scope_root) / ROUTE_STATE_FILENAME)
        )
        self.assertEqual(ROUTE_STATE_FILENAME, "route_state.json")

    def test_lkg_authority_store_path_is_exact(self) -> None:
        scope = self.scope()
        self.assertEqual(
            scope.lkg_authority_store_path,
            str(Path(scope.scope_root) / LKG_AUTHORITY_STORE_FILENAME),
        )
        self.assertEqual(LKG_AUTHORITY_STORE_FILENAME, "lkg_authority.sqlite3")

    def test_runs_root_is_exact(self) -> None:
        scope = self.scope()
        self.assertEqual(scope.runs_root, str(Path(scope.scope_root) / RUNS_DIRECTORY))
        self.assertEqual(RUNS_DIRECTORY, "runs")

    def test_run_root_is_runs_root_plus_source_run_id(self) -> None:
        scope = self.scope()
        self.assertEqual(
            scope.run_root(_RUN_ID), str(Path(scope.runs_root) / _RUN_ID)
        )

    def test_a_different_source_run_id_is_a_different_run_root(self) -> None:
        scope = self.scope()
        self.assertNotEqual(scope.run_root(_RUN_ID), scope.run_root("other-run-0002"))
        for root in (scope.run_root(_RUN_ID), scope.run_root("other-run-0002")):
            self.assertTrue(root.startswith(scope.scope_root + os.sep))

    def test_run_root_refuses_a_traversing_component(self) -> None:
        """A path helper must never compose an escaping path, whatever reaches it."""

        scope = self.scope()
        for hostile in ("..", ".", "a/b", "/abs", "", "a\x00b"):
            with self.subTest(source_run_id=hostile):
                with self.assertRaises(DeploymentGovernanceError) as caught:
                    scope.run_root(hostile)
                self.assertEqual(caught.exception.code, "DEPLOYMENT_RUN_ID_INVALID")

    def test_derivation_creates_nothing(self) -> None:
        """Preflight and prepare derive the whole scope and must still write nothing."""

        scope = self.scope()
        for path in (
            scope.canonical_root,
            scope.scope_root,
            scope.route_state_path,
            scope.lkg_authority_store_path,
            scope.runs_root,
            scope.run_root(_RUN_ID),
        ):
            self.assertFalse(Path(path).exists(), path)

    def test_repeated_derivation_is_identical(self) -> None:
        self.assertEqual(self.scope(), self.scope())

    def test_scope_is_immutable(self) -> None:
        scope = self.scope()
        with self.assertRaises(Exception):
            scope.scope_root = "/elsewhere"  # type: ignore[misc]

    def test_lexical_aliases_of_the_root_resolve_to_one_scope(self) -> None:
        """``realpath`` anchoring removes the alias variance a lexical parent had."""

        self.root.mkdir(parents=True, mode=0o700)
        alias = Path(str(self.root) + "/./")
        dotdot = self.root.parent / self.root.name / ".." / self.root.name
        for candidate in (alias, dotdot):
            with self.subTest(candidate=str(candidate)):
                self.assertEqual(
                    resolve_deployment_governance_scope(canonical_root=candidate),
                    self.scope(),
                )

    def test_a_relative_root_is_refused_rather_than_joined_with_the_cwd(self) -> None:
        """The scope must never depend on where the process happened to start."""

        for relative in ("relative/governance", "./gov", "gov"):
            with self.subTest(root=relative):
                with self.assertRaises(DeploymentGovernanceError) as caught:
                    resolve_deployment_governance_scope(canonical_root=relative)
                self.assertEqual(caught.exception.code, "DEPLOYMENT_ROOT_INVALID")

    def test_symlinked_root_resolves_to_the_same_scope(self) -> None:
        self.root.mkdir(parents=True, mode=0o700)
        link = Path(self._temporary.name) / "alias-governance"
        os.symlink(self.root, link)
        self.assertEqual(
            resolve_deployment_governance_scope(canonical_root=link), self.scope()
        )


# ======================================================================
# 12-15. Scope directory hardening
# ======================================================================


class ScopeHardeningTests(_TempRootCase):
    def test_scope_directory_is_created_private(self) -> None:
        scope = self.scope()
        ensure_deployment_scope_directory(scope)
        mode = stat.S_IMODE(os.lstat(scope.scope_root).st_mode)
        self.assertEqual(mode & 0o077, 0, oct(mode))
        self.assertTrue(Path(scope.scope_root).is_dir())

    def test_creation_is_idempotent(self) -> None:
        scope = self.scope()
        ensure_deployment_scope_directory(scope)
        ensure_deployment_scope_directory(scope)
        self.assertTrue(Path(scope.scope_root).is_dir())

    def test_group_or_world_accessible_scope_is_refused_not_repaired(self) -> None:
        scope = self.scope()
        Path(scope.scope_root).mkdir(parents=True, mode=0o700)
        os.chmod(scope.scope_root, 0o755)
        with self.assertRaises(DeploymentGovernanceError) as caught:
            ensure_deployment_scope_directory(scope)
        self.assertEqual(caught.exception.code, "DEPLOYMENT_SCOPE_NOT_PRIVATE")
        self.assertEqual(
            stat.S_IMODE(os.lstat(scope.scope_root).st_mode),
            0o755,
            "an unsafe scope is refused, never silently chmodded",
        )

    def test_a_regular_file_standing_where_the_scope_belongs_is_refused(self) -> None:
        scope = self.scope()
        Path(scope.scope_root).parent.mkdir(parents=True, mode=0o700)
        Path(scope.scope_root).write_bytes(b"not a directory")
        with self.assertRaises(DeploymentGovernanceError) as caught:
            ensure_deployment_scope_directory(scope)
        self.assertEqual(caught.exception.code, "DEPLOYMENT_SCOPE_UNUSABLE")

    def test_a_symlink_standing_where_the_scope_belongs_is_refused(self) -> None:
        """An alias is never the authority, even when it points at a directory."""

        scope = self.scope()
        elsewhere = Path(self._temporary.name) / "elsewhere"
        elsewhere.mkdir(mode=0o700)
        Path(scope.scope_root).parent.mkdir(parents=True, mode=0o700)
        os.symlink(elsewhere, scope.scope_root)
        with self.assertRaises(DeploymentGovernanceError) as caught:
            ensure_deployment_scope_directory(scope)
        self.assertEqual(caught.exception.code, "DEPLOYMENT_SCOPE_NOT_A_DIRECTORY")


# ======================================================================
# 21-22. One deployment, one scope; no production override
# ======================================================================


class ProductionAuthorityTests(_TempRootCase):
    def test_production_scope_uses_the_canonical_root_and_identity(self) -> None:
        scope = canonical_deployment_governance_scope()
        self.assertEqual(
            scope.deployment_identity, CANONICAL_ENV001_DEPLOYMENT_IDENTITY
        )
        self.assertEqual(
            scope.canonical_root,
            os.path.realpath(
                Path(pwd.getpwuid(os.getuid()).pw_dir).joinpath(".local", "share", "vd")
            ),
        )
        self.assertEqual(
            scope.namespace_digest,
            deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY),
        )

    def test_the_production_scope_is_a_pure_function_of_source_authority(self) -> None:
        """One logical deployment -> one governance scope, every time."""

        self.assertEqual(
            canonical_deployment_governance_scope(),
            canonical_deployment_governance_scope(),
        )

    def test_two_distinct_deployments_are_isolated(self) -> None:
        first = self.scope()
        second = resolve_deployment_governance_scope(
            deployment_identity="ENV-002-probe", canonical_root=self.root
        )
        self.assertNotEqual(first.namespace_digest, second.namespace_digest)
        for attribute in (
            "scope_root",
            "route_state_path",
            "lkg_authority_store_path",
            "runs_root",
        ):
            self.assertNotEqual(
                getattr(first, attribute), getattr(second, attribute), attribute
            )
        self.assertNotEqual(first.run_root(_RUN_ID), second.run_root(_RUN_ID))

    def test_an_injected_test_root_cannot_reach_the_production_operator(self) -> None:
        """The seam is in-process only; no operator input names a root or identity."""

        import vdbench.lkg_qualification_operator as operator

        for field in (
            "route_state_path",
            "lkg_authority_store_path",
            "run_root",
            "deployment_identity",
            "governance_root",
            "canonical_root",
            "state_root",
            "deployment_root",
        ):
            self.assertNotIn(field, operator.OPERAND_FIELDS, field)

        options = {
            option
            for action in operator._parser()._actions
            for option in action.option_strings
        }
        for forbidden in (
            "--governance-root",
            "--deployment-identity",
            "--route-state-path",
            "--lkg-authority-store-path",
            "--state-root",
        ):
            self.assertNotIn(forbidden, options, forbidden)

        source = Path("src/vdbench/lkg_qualification_operator.py").read_text()
        self.assertNotIn("os.environ", source)
        self.assertNotIn("getenv", source)

    def test_the_scope_type_is_required_at_the_injection_seam(self) -> None:
        """A stray string cannot be smuggled in where a scope belongs."""

        import vdbench.lkg_qualification_operator as operator

        with self.assertRaises(operator.LkgQualificationOperatorError) as caught:
            operator._resolve_scope("/tmp/not-a-scope")  # type: ignore[arg-type]
        self.assertEqual(caught.exception.code, "LKG_GOVERNANCE_SCOPE_INVALID")
        self.assertIsInstance(
            operator._resolve_scope(None), DeploymentGovernanceScope
        )


if __name__ == "__main__":
    unittest.main()


# ======================================================================
# ADR-023. HOME-invariance: the process environment is not authority
# ======================================================================


_DERIVE_PROGRAM = """
import json, os, pwd, sys
sys.path.insert(0, {src!r})
from vdbench.deployment_governance import (
    canonical_account_home, canonical_vd_governance_root,
    canonical_deployment_governance_scope)
scope = canonical_deployment_governance_scope()
json.dump({{
    "uid": os.getuid(),
    "account_home": str(canonical_account_home()),
    "governance_root": str(canonical_vd_governance_root()),
    "deployment_identity": scope.deployment_identity,
    "namespace_digest": scope.namespace_digest,
    "canonical_root": scope.canonical_root,
    "scope_root": scope.scope_root,
    "route_state_path": scope.route_state_path,
    "lkg_authority_store_path": scope.lkg_authority_store_path,
    "runs_root": scope.runs_root,
    "run_root": scope.run_root({run_id!r}),
}}, sys.stdout)
"""


class HomeInvarianceTests(unittest.TestCase):
    '''ADR-023: same uid + same source + any HOME -> one governance scope.

    Subprocesses are mandatory here, not stylistic. The defect this closes was
    an IMPORT-TIME ``Path.home()`` constant, and an in-process test can never
    observe import-time behaviour under a different environment: the module is
    already imported. Each case therefore re-executes a fresh interpreter.

    No expected value is hardcoded. The expectation is computed by an
    INDEPENDENT test-side account lookup, so this suite is portable to any
    account and cannot pass by agreeing with a committed absolute pathname.
    '''

    @classmethod
    def setUpClass(cls) -> None:
        cls._source_root = str(Path(__file__).resolve().parent.parent / "src")
        cls._program = _DERIVE_PROGRAM.format(
            src=cls._source_root, run_id=_RUN_ID
        )

    def _derive(self, *, home: str | None) -> dict[str, object]:
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        if home is None:
            environment.pop("HOME", None)
        else:
            environment["HOME"] = home
        completed = subprocess.run(
            [sys.executable, "-c", self._program],
            capture_output=True,
            text=True,
            check=True,
            env=environment,
        )
        return json.loads(completed.stdout)

    def test_every_home_yields_one_identical_governance_scope(self) -> None:
        """A. real login home, B/C. arbitrary temp homes, D. HOME unset."""

        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            actual = pwd.getpwuid(os.getuid()).pw_dir
            self.assertNotEqual(first, second)
            self.assertNotEqual(first, actual)
            derivations = {
                "actual_login_home": self._derive(home=actual),
                "temporary_home_a": self._derive(home=first),
                "temporary_home_b": self._derive(home=second),
                "home_unset": self._derive(home=None),
            }

        expected_home = pwd.getpwuid(os.getuid()).pw_dir
        expected_root = str(Path(expected_home).joinpath(*CANONICAL_VD_GOVERNANCE_ROOT_SUFFIX))
        for case, derived in derivations.items():
            with self.subTest(case=case):
                self.assertEqual(derived["uid"], os.getuid())
                self.assertEqual(derived["account_home"], expected_home)
                self.assertEqual(derived["governance_root"], expected_root)

        # Every governed field, not merely the root, must be identical.
        reference = derivations["actual_login_home"]
        for field in (
            "account_home",
            "governance_root",
            "deployment_identity",
            "namespace_digest",
            "canonical_root",
            "scope_root",
            "route_state_path",
            "lkg_authority_store_path",
            "runs_root",
            "run_root",
        ):
            values = {case: derived[field] for case, derived in derivations.items()}
            with self.subTest(field=field):
                self.assertEqual(
                    len(set(values.values())), 1, f"{field} varied with HOME: {values}"
                )

    def test_a_spoofed_home_does_not_change_the_namespace_digest(self) -> None:
        """The digest was already HOME-invariant; this pins it against regression."""

        with tempfile.TemporaryDirectory() as spoofed:
            derived = self._derive(home=spoofed)
        self.assertEqual(
            derived["namespace_digest"],
            deployment_namespace_digest(CANONICAL_ENV001_DEPLOYMENT_IDENTITY),
        )
        self.assertEqual(
            derived["deployment_identity"], CANONICAL_ENV001_DEPLOYMENT_IDENTITY
        )

    def test_a_spoofed_home_creates_no_state_under_the_spoofed_root(self) -> None:
        """Derivation is a read; a spoofed HOME must not become a scaffold."""

        with tempfile.TemporaryDirectory() as spoofed:
            self._derive(home=spoofed)
            self.assertEqual(sorted(Path(spoofed).iterdir()), [])


class AccountHomeFailureTests(unittest.TestCase):
    '''ADR-023: account-home resolution fails CLOSED, with no substitute root.

    The deliberate failure tests. Patching is applied at the lowest possible
    boundary -- ``pwd.getpwuid`` -- so the module's own validation, not a stub
    of it, is what refuses. No real system account database is touched.
    '''

    def test_account_database_lookup_failure_refuses(self) -> None:
        for failure in (KeyError("no such uid"), OSError("nss unavailable")):
            with self.subTest(failure=type(failure).__name__):
                with mock.patch("pwd.getpwuid", side_effect=failure):
                    with self.assertRaises(DeploymentGovernanceError) as caught:
                        canonical_account_home()
                self.assertEqual(
                    caught.exception.code, "DEPLOYMENT_ACCOUNT_HOME_UNRESOLVED"
                )

    def test_empty_relative_or_malformed_account_home_refuses(self) -> None:
        for label, home in (
            ("empty", ""),
            ("relative", "relative/home"),
            ("bare_component", "home"),
            ("wrong_type_none", None),
            ("wrong_type_bytes", b"/Users/someone"),
            ("wrong_type_path", Path("/Users/someone")),
        ):
            with self.subTest(case=label):
                with mock.patch(
                    "pwd.getpwuid", return_value=_FakeAccountRecord(home)
                ):
                    with self.assertRaises(DeploymentGovernanceError) as caught:
                        canonical_account_home()
                self.assertEqual(
                    caught.exception.code, "DEPLOYMENT_ACCOUNT_HOME_INVALID"
                )

    def test_a_record_without_pw_dir_refuses(self) -> None:
        with mock.patch("pwd.getpwuid", return_value=object()):
            with self.assertRaises(DeploymentGovernanceError) as caught:
                canonical_account_home()
        self.assertEqual(caught.exception.code, "DEPLOYMENT_ACCOUNT_HOME_INVALID")

    def test_failure_never_falls_back_to_home_expanduser_or_cwd(self) -> None:
        """The whole point: a refusal must not be repaired into a second root.

        HOME, expanduser and the working directory are all set to plausible,
        writable decoys. Any fallback would return one of them instead of
        raising, and would be a second governance universe for this account.
        """

        with tempfile.TemporaryDirectory() as decoy:
            with mock.patch.dict(
                os.environ, {"HOME": decoy, "XDG_DATA_HOME": decoy}, clear=False
            ):
                with mock.patch("pwd.getpwuid", side_effect=KeyError("no account")):
                    for callable_under_test in (
                        canonical_account_home,
                        canonical_vd_governance_root,
                        canonical_deployment_governance_scope,
                        resolve_deployment_governance_scope,
                    ):
                        with self.subTest(callable=callable_under_test.__name__):
                            with self.assertRaises(DeploymentGovernanceError) as caught:
                                callable_under_test()
                            self.assertEqual(
                                caught.exception.code,
                                "DEPLOYMENT_ACCOUNT_HOME_UNRESOLVED",
                            )
            self.assertEqual(sorted(Path(decoy).iterdir()), [])

    def test_an_injected_root_still_bypasses_the_account_lookup(self) -> None:
        """Test isolation must survive a broken account database.

        The injected-scope seam exists so tests never touch real deployment
        state; it would be self-defeating if it depended on the very lookup
        whose failure modes are under test.
        """

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch("pwd.getpwuid", side_effect=KeyError("no account")):
                scope = resolve_deployment_governance_scope(canonical_root=directory)
        self.assertTrue(scope.scope_root.startswith(os.path.realpath(directory)))

    def test_a_tilde_in_an_injected_root_is_not_expanded(self) -> None:
        """ADR-023: expanduser is not applied anywhere, injected roots included."""

        with self.assertRaises(DeploymentGovernanceError) as caught:
            resolve_deployment_governance_scope(canonical_root="~/vd-governance")
        self.assertEqual(caught.exception.code, "DEPLOYMENT_ROOT_INVALID")


class _FakeAccountRecord:
    """The narrowest possible stand-in for one ``pwd.struct_passwd`` field."""

    def __init__(self, pw_dir: object) -> None:
        self.pw_dir = pw_dir
