"""Offline tests for the EXP-009 in-memory one-shot route authority."""

from __future__ import annotations

from dataclasses import dataclass
import threading
import unittest

from vdbench.canary_route_authority import CanaryRouteAuthority, RouteAuthorityState
from vdbench.canary_routing import CanaryRouteKind, RouteResolution
from vdbench.canary_route_state import RouteState, RouteStateBinding, RouteStateRecord
from vdbench.config import Metric


@dataclass(frozen=True, slots=True)
class FakePlan:
    plan_sha256: str = "a" * 64
    metric: Metric = Metric.L2
    threshold_stratum: str = "target-075"
    last_known_good_ef: int = 400
    configuration_identity: str = "config-v1"
    data_identity: str = "data-v1"
    flat_binding_id: str = "flat-v1"
    hnsw_binding_id: str = "hnsw-v1"

    def resolve(self, occurrence_id: object) -> RouteResolution:
        if occurrence_id == "exp009-routing-000000":
            return RouteResolution(True, str(occurrence_id), 0, 800, CanaryRouteKind.CANDIDATE)
        if occurrence_id == "exp009-routing-000001":
            return RouteResolution(True, str(occurrence_id), 1, 400, CanaryRouteKind.LAST_KNOWN_GOOD)
        return RouteResolution(False, None, None, None, None, "OCCURRENCE_UNKNOWN")


class CanaryRouteAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = FakePlan()
        self.binding = RouteStateBinding(
            metric=Metric.L2, threshold_stratum="target-075", last_known_good_ef=400,
            configuration_identity="config-v1", data_identity="data-v1",
            flat_binding_id="flat-v1", hnsw_binding_id="hnsw-v1",
        )
        self.marker = RouteStateRecord(
            state=RouteState.ACTIVATING, binding=self.binding,
            grant_id="grant-exp009-001", plan_sha256=self.plan.plan_sha256,
            changed_at_utc="2026-08-04T08:00:00Z", reason_code="ACTIVATION_PENDING",
        )

    def test_inactive_authority_refuses_without_a_route(self) -> None:
        claim = CanaryRouteAuthority().resolve_and_claim("exp009-routing-000000")
        self.assertFalse(claim.accepted)
        self.assertEqual(claim.reason_code, "ROUTE_INACTIVE")
        self.assertIsNone(claim.ef)

    def test_activation_binds_marker_then_claims_each_occurrence_once(self) -> None:
        authority = CanaryRouteAuthority()
        snapshot = authority.activate(plan=self.plan, activation_marker=self.marker)
        candidate = authority.resolve_and_claim("exp009-routing-000000")
        lkg = authority.resolve_and_claim("exp009-routing-000001")
        duplicate = authority.resolve_and_claim("exp009-routing-000000")

        self.assertEqual(snapshot.state, RouteAuthorityState.ACTIVE)
        self.assertEqual(snapshot.grant_id, "grant-exp009-001")
        self.assertTrue(candidate.accepted)
        self.assertEqual(candidate.ef, 800)
        self.assertTrue(lkg.accepted)
        self.assertEqual(lkg.ef, 400)
        self.assertFalse(duplicate.accepted)
        self.assertEqual(duplicate.reason_code, "OCCURRENCE_ALREADY_CLAIMED")

    def test_marker_plan_identity_mismatch_refuses_activation(self) -> None:
        authority = CanaryRouteAuthority()
        invalid = RouteStateRecord(
            state=RouteState.ACTIVATING,
            binding=RouteStateBinding(
                metric=Metric.L2, threshold_stratum="target-075", last_known_good_ef=400,
                configuration_identity="wrong", data_identity="data-v1",
                flat_binding_id="flat-v1", hnsw_binding_id="hnsw-v1",
            ),
            grant_id="grant-exp009-001", plan_sha256=self.plan.plan_sha256,
            changed_at_utc="2026-08-04T08:00:00Z", reason_code="ACTIVATION_PENDING",
        )
        with self.assertRaisesRegex(ValueError, "ACTIVATION_MARKER_MISMATCH"):
            authority.activate(plan=self.plan, activation_marker=invalid)
        self.assertEqual(authority.snapshot().state, RouteAuthorityState.LKG_ONLY)

    def test_clear_drops_claims_and_the_entire_plan(self) -> None:
        authority = CanaryRouteAuthority()
        authority.activate(plan=self.plan, activation_marker=self.marker)
        authority.resolve_and_claim("exp009-routing-000000")
        cleared = authority.clear(reason_code="EXPLICIT_REMOVAL")

        self.assertEqual(cleared.state, RouteAuthorityState.LKG_ONLY)
        self.assertIsNone(cleared.plan_sha256)
        self.assertEqual(cleared.claimed_occurrence_count, 0)
        self.assertEqual(
            authority.resolve_and_claim("exp009-routing-000000").reason_code,
            "ROUTE_INACTIVE",
        )

    def test_unknown_occurrence_is_refused_without_consuming_a_claim(self) -> None:
        authority = CanaryRouteAuthority()
        authority.activate(plan=self.plan, activation_marker=self.marker)

        unknown = authority.resolve_and_claim("exp009-routing-999999")

        self.assertFalse(unknown.accepted)
        self.assertEqual(unknown.reason_code, "OCCURRENCE_UNKNOWN")
        self.assertEqual(authority.snapshot().claimed_occurrence_count, 0)

    def test_concurrent_duplicate_claim_has_exactly_one_winner(self) -> None:
        authority = CanaryRouteAuthority()
        authority.activate(plan=self.plan, activation_marker=self.marker)
        barrier = threading.Barrier(2)
        results = []

        def claim() -> None:
            barrier.wait(timeout=2.0)
            results.append(authority.resolve_and_claim("exp009-routing-000000"))

        first, second = threading.Thread(target=claim), threading.Thread(target=claim)
        first.start(); second.start(); first.join(timeout=3.0); second.join(timeout=3.0)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(sum(result.accepted for result in results), 1)
        self.assertEqual(
            [result.reason_code for result in results if not result.accepted],
            ["OCCURRENCE_ALREADY_CLAIMED"],
        )


if __name__ == "__main__":
    unittest.main()
