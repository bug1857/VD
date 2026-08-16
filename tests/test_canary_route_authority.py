"""Offline tests for the EXP-009 in-memory one-shot route authority."""

from __future__ import annotations

import threading
import unittest
from dataclasses import dataclass
from datetime import UTC, datetime

from vdbench.canary_route_authority import CanaryRouteAuthority, RouteAuthorityState
from vdbench.canary_route_state import RouteState, RouteStateBinding, RouteStateRecord
from vdbench.canary_routing import CanaryRouteKind, RouteResolution
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


class MutableUtcClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class CanaryRouteAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = MutableUtcClock(datetime(2026, 8, 4, 8, 0, tzinfo=UTC))
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

    def _authority(self) -> CanaryRouteAuthority:
        return CanaryRouteAuthority(clock=self.clock)

    def test_inactive_authority_refuses_without_a_route(self) -> None:
        claim = self._authority().resolve_and_claim("exp009-routing-000000")
        self.assertFalse(claim.accepted)
        self.assertEqual(claim.reason_code, "ROUTE_INACTIVE")
        self.assertIsNone(claim.ef)

    def test_activation_binds_marker_then_claims_each_occurrence_once(self) -> None:
        authority = self._authority()
        snapshot = authority.activate(
            plan=self.plan,
            activation_marker=self.marker,
            expires_at_utc="2026-08-04T08:30:00Z",
        )
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
        authority = self._authority()
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
            authority.activate(
                plan=self.plan,
                activation_marker=invalid,
                expires_at_utc="2026-08-04T08:30:00Z",
            )
        self.assertEqual(authority.snapshot().state, RouteAuthorityState.LKG_ONLY)

    def test_clear_drops_claims_and_the_entire_plan(self) -> None:
        authority = self._authority()
        authority.activate(
            plan=self.plan,
            activation_marker=self.marker,
            expires_at_utc="2026-08-04T08:30:00Z",
        )
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
        authority = self._authority()
        authority.activate(
            plan=self.plan,
            activation_marker=self.marker,
            expires_at_utc="2026-08-04T08:30:00Z",
        )

        unknown = authority.resolve_and_claim("exp009-routing-999999")

        self.assertFalse(unknown.accepted)
        self.assertEqual(unknown.reason_code, "OCCURRENCE_UNKNOWN")
        self.assertEqual(authority.snapshot().claimed_occurrence_count, 0)

    def test_concurrent_duplicate_claim_has_exactly_one_winner(self) -> None:
        authority = self._authority()
        authority.activate(
            plan=self.plan,
            activation_marker=self.marker,
            expires_at_utc="2026-08-04T08:30:00Z",
        )
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

    def test_expired_lease_clears_plan_before_any_candidate_route_can_be_claimed(self) -> None:
        authority = self._authority()
        authority.activate(
            plan=self.plan,
            activation_marker=self.marker,
            expires_at_utc="2026-08-04T08:01:00Z",
        )
        self.clock.now = datetime(2026, 8, 4, 8, 1, tzinfo=UTC)

        claim = authority.resolve_and_claim("exp009-routing-000000")
        snapshot = authority.snapshot()

        self.assertFalse(claim.accepted)
        self.assertIsNone(claim.dataset_query_id)
        self.assertIsNone(claim.ef)
        self.assertEqual(claim.reason_code, "ROUTE_APPROVAL_EXPIRED")
        self.assertEqual(snapshot.state, RouteAuthorityState.LKG_ONLY)
        self.assertEqual(snapshot.reason_code, "ROUTE_APPROVAL_EXPIRED")
        self.assertEqual(snapshot.claimed_occurrence_count, 0)

    def test_expired_grant_is_refused_at_publication(self) -> None:
        authority = self._authority()
        self.clock.now = datetime(2026, 8, 4, 8, 30, tzinfo=UTC)

        with self.assertRaisesRegex(ValueError, "ROUTE_APPROVAL_EXPIRED"):
            authority.activate(
                plan=self.plan,
                activation_marker=self.marker,
                expires_at_utc="2026-08-04T08:30:00Z",
            )
        self.assertEqual(authority.snapshot().state, RouteAuthorityState.LKG_ONLY)

    def test_invalid_clock_fails_closed_before_any_route_claim(self) -> None:
        authority = self._authority()
        authority.activate(
            plan=self.plan,
            activation_marker=self.marker,
            expires_at_utc="2026-08-04T08:30:00Z",
        )
        self.clock.now = "not-a-utc-datetime"  # type: ignore[assignment]

        claim = authority.resolve_and_claim("exp009-routing-000000")

        self.assertFalse(claim.accepted)
        self.assertIsNone(claim.dataset_query_id)
        self.assertIsNone(claim.ef)
        self.assertEqual(claim.reason_code, "ROUTE_CLOCK_UNAVAILABLE")
        self.assertEqual(authority.snapshot().state, RouteAuthorityState.LKG_ONLY)


if __name__ == "__main__":
    unittest.main()
