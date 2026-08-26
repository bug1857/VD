from __future__ import annotations

import copy
import hashlib
import unittest
from dataclasses import fields, replace
from unittest import mock

from vdbench.exp012_scale_campaign import Exp012ScaleCampaignBinding
from vdbench.exp012_scale_contract import (
    Exp012ScaleProfile,
    build_exp012_scale_contract,
    exp012_scale_contract_payload,
)
from vdbench.gate_c_bounded_execution import (
    GateCBoundedExecutionError,
    GateCWindowExecutionBound,
    build_gate_c_bounded_execution_envelope,
    build_gate_c_bounded_execution_envelope_v2,
    build_gate_c_bounded_execution_envelope_v3,
    build_gate_c_canonical_state,
    build_gate_c_checkpoint_result,
    build_gate_c_checkpoint_result_v2,
    build_gate_c_checkpoint_result_v3,
    build_gate_c_window_checkpoint_effect,
    gate_c_bounded_execution_envelope_document,
    gate_c_bounded_execution_envelope_document_v2,
    gate_c_bounded_execution_envelope_document_v3,
    gate_c_bounded_execution_envelope_payload,
    verify_gate_c_bounded_execution_envelope,
    verify_gate_c_bounded_execution_envelope_v2,
    verify_gate_c_bounded_execution_envelope_v3,
    verify_gate_c_checkpoint_result,
    verify_gate_c_checkpoint_result_v2,
    verify_gate_c_checkpoint_result_v3,
    verify_gate_c_window_execution_bound,
)
from tests.test_gate_c_execution_environment import (
    _governed as _environment_governed,
    environment_fixture,
)
from vdbench.gate_c_window_execution import GateCWindowExecutionError
from vdbench.host_window_lineage import CommittedHostObservation, VerifiedHostSourceHead
from vdbench.artifacts import canonical_json_bytes
from vdbench.canonical_serialization import strict_canonical_digest
from vdbench.config import Metric
from vdbench.shadow_event_types import MonitorStreamKey


def _sources(
    count: int,
    *,
    producer: str = "a" * 32,
    source_revision: str = "revision",
):
    result = []
    stream = MonitorStreamKey(
        "stream", Metric.L2, "target-075", "cfg", "data", "flat", "hnsw"
    )
    for sequence in range(count):
        source = object.__new__(CommittedHostObservation)
        object.__setattr__(source, "source_sequence", sequence)
        object.__setattr__(source, "query_id", f"logsim-v2:{producer}:{sequence}")
        object.__setattr__(source, "stream_key", stream)
        object.__setattr__(source, "source_revision", source_revision)
        object.__setattr__(source, "environment_manifest_sha256", "e" * 64)
        object.__setattr__(source, "source_sha256", f"{sequence + 1:064x}"[-64:])
        result.append(source)
    return tuple(result)


def _fixture(
    *,
    start: int = 0,
    count: int = 1,
    target_profile=Exp012ScaleProfile.SCALE_2400,
    source_revision: str = "revision",
):
    contract = build_exp012_scale_contract(target_profile)
    plan = {
        "schema_version": "exp012-scale-gate-c-plan-v1",
        "experiment_id": "EXP-012-SCALE",
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "stream": {
            "stream_id": "stream", "metric": "L2",
            "threshold_stratum": "target-075",
            "configuration_identity": "cfg", "data_identity": "data",
            "flat_binding_id": "flat", "hnsw_binding_id": "hnsw",
        },
        "source_revision": source_revision,
        "environment_manifest_sha256": "e" * 64,
        "gate_a_authority": {"evidence_sha256": "a" * 64},
        "stores": {"root": "/tmp/campaign/stores"},
        "observed": {
            "source_count": contract.target_source_records,
            "complete_source_windows": contract.expected_windows,
            "next_window_sequence": start,
        },
    }
    plan["plan_sha256"] = strict_canonical_digest(
        b"VD::EXP012_SCALE_GATE_C_PLAN::V1\x00", plan
    )
    campaign_payload = {
        "schema_version": "exp012-scale-campaign-v1",
        "experiment_id": "EXP-012-SCALE",
        "scale_contract": exp012_scale_contract_payload(contract),
        "scale_contract_sha256": contract.contract_sha256,
        "gate_a_evidence_sha256": "a" * 64,
    }
    campaign = Exp012ScaleCampaignBinding(
        contract,
        "a" * 64,
        strict_canonical_digest(
            b"VD::EXP012_SCALE_CAMPAIGN::V1\x00", campaign_payload
        ),
    )
    sources = _sources(
        contract.target_source_records,
        source_revision=source_revision,
    )
    head_payload = {
        "schema_version": "response-profile-host-verified-head-v1",
        "source_count": contract.target_source_records,
        "maximum_source_sequence": contract.target_source_records - 1,
        "source_head_sha256": sources[-1].source_sha256,
        "outbox_head_sha256": "2" * 64,
        "store_binding_sha256": "3" * 64,
    }
    head = VerifiedHostSourceHead(
        source_count=contract.target_source_records,
        maximum_source_sequence=contract.target_source_records - 1,
        source_head_sha256=sources[-1].source_sha256,
        outbox_head_sha256="2" * 64,
        store_binding_sha256="3" * 64,
        head_snapshot_sha256=hashlib.sha256(
            b"VD::HOST_RESPONSE_VERIFIED_HEAD::V1\x00"
            + canonical_json_bytes(head_payload)
        ).hexdigest(),
    )
    bound = GateCWindowExecutionBound(start, count)
    envelope = build_gate_c_bounded_execution_envelope(
        plan=plan, campaign_binding=campaign, source_head=head,
        sources=sources, execution_bound=bound,
    )
    return plan, campaign, head, sources, envelope


def _fixture_v2(
    *,
    start: int = 0,
    count: int = 1,
    target_profile=Exp012ScaleProfile.SCALE_2400,
    source_revision: str = "revision",
    execution_source_revision: str = "b" * 40,
):
    plan, campaign, head, sources, _legacy = _fixture(
        start=start,
        count=count,
        target_profile=target_profile,
        source_revision=source_revision,
    )
    envelope = build_gate_c_bounded_execution_envelope_v2(
        plan=plan,
        campaign_binding=campaign,
        source_head=head,
        sources=sources,
        execution_bound=GateCWindowExecutionBound(start, count),
        execution_source_revision=execution_source_revision,
    )
    return plan, campaign, head, sources, envelope


def _fixture_v3(
    *, start: int = 0, count: int = 1,
    target_profile=Exp012ScaleProfile.SCALE_2400,
    source_revision: str = "3" * 40,
    execution_source_revision: str = "5" * 40,
):
    plan, campaign, head, sources, v2 = _fixture_v2(
        start=start,
        count=count,
        target_profile=target_profile,
        source_revision=source_revision,
        execution_source_revision=execution_source_revision,
    )
    governed = _environment_governed()
    governed.update({
        "campaign_identity": v2.campaign_identity,
        "scale_contract_sha256": v2.scale_contract.contract_sha256,
        "gate_a_evidence_sha256": v2.gate_a_evidence_sha256,
        "source_revision": v2.source_revision,
        "environment_manifest_sha256": v2.environment_manifest_sha256,
        "data_identity": v2.data_identity,
        "configuration_identity": v2.configuration_identity,
        "flat_binding_id": v2.flat_binding_id,
        "hnsw_binding_id": v2.hnsw_binding_id,
        "metric": v2.metric,
        "dimensions": 128,
        "expected_entity_count": 10_000,
        "served_ef": 400,
        "consistency_level": "Strong",
        "flat_collection_name": "flat_collection",
        "hnsw_collection_name": "hnsw_collection",
    })
    governed["flat_gate_a_binding"]["binding_id"] = v2.flat_binding_id
    governed["hnsw_gate_a_binding"]["binding_id"] = v2.hnsw_binding_id
    *_unused, attestation = environment_fixture(
        governed=governed,
        execution_source_revision=execution_source_revision,
    )
    envelope = build_gate_c_bounded_execution_envelope_v3(
        plan=plan,
        campaign_binding=campaign,
        source_head=head,
        sources=sources,
        execution_bound=GateCWindowExecutionBound(start, count),
        execution_source_revision=execution_source_revision,
        execution_environment_attestation=attestation,
    )
    return plan, campaign, head, sources, envelope


def _forged_envelope_with_bound(envelope, bound):
    forged = object.__new__(type(envelope))
    for field in fields(type(envelope)):
        object.__setattr__(forged, field.name, getattr(envelope, field.name))
    object.__setattr__(forged, "execution_bound", bound)
    return forged


class GateCBoundedExecutionTests(unittest.TestCase):
    def test_v3_envelope_golden_digest_is_frozen(self) -> None:
        *_unused, envelope = _fixture_v3()
        self.assertEqual(
            envelope.envelope_sha256,
            "626d65c39e33310580df5203d2541385eec5c9e9e8954f040826cd03155aef9a",
        )

    def test_v3_round_trip_binds_independent_runtime_and_attestation(self) -> None:
        plan, campaign, head, sources, envelope = _fixture_v3()
        document = gate_c_bounded_execution_envelope_document_v3(envelope)
        verified = verify_gate_c_bounded_execution_envelope_v3(
            document,
            plan=plan,
            campaign_binding=campaign,
            source_head=head,
            sources=sources,
            execution_source_revision="5" * 40,
        )
        self.assertEqual(verified.envelope_sha256, envelope.envelope_sha256)
        payload = document["envelope_payload"]
        self.assertEqual(payload["source_revision"], "3" * 40)
        self.assertEqual(payload["execution_source_revision"], "5" * 40)
        self.assertEqual(
            payload["execution_environment_identity_sha256"],
            envelope.execution_environment_identity_sha256,
        )
        self.assertEqual(
            payload["execution_environment_attestation_sha256"],
            envelope.execution_environment_attestation_sha256,
        )

    def test_v3_refuses_v2_documents_and_attestation_substitution(self) -> None:
        plan, campaign, head, sources, v3 = _fixture_v3()
        *_unused, v2 = _fixture_v2(source_revision="3" * 40)
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_bounded_execution_envelope_v3(
                gate_c_bounded_execution_envelope_document_v2(v2),
                plan=plan,
                campaign_binding=campaign,
                source_head=head,
                sources=sources,
                execution_source_revision="5" * 40,
            )
        tampered = copy.deepcopy(
            gate_c_bounded_execution_envelope_document_v3(v3)
        )
        tampered["envelope_payload"][
            "execution_environment_attestation_sha256"
        ] = "0" * 64
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_bounded_execution_envelope_v3(
                tampered,
                plan=plan,
                campaign_binding=campaign,
                source_head=head,
                sources=sources,
                execution_source_revision="5" * 40,
            )

    def test_v3_result_is_versioned_and_reconstructive(self) -> None:
        *_unused, envelope = _fixture_v3()
        pre = build_gate_c_canonical_state(
            next_window_sequence=0,
            acknowledgement_count=0,
            acknowledgement_head_sha256=None,
            attempt_count=0,
            attempt_event_count=0,
            attempt_event_head_sha256=None,
            detector_event_count=0,
            detector_event_head_sha256=None,
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=0,
            finalization_event_count=0,
            finalization_event_head_sha256=None,
            telemetry_record_count=0,
            telemetry_record_head_sha256=None,
        )
        effect = build_gate_c_window_checkpoint_effect(
            window_sequence=0,
            source_window_sha256="1" * 64,
            attempt_sha256s=tuple(f"{index:064x}" for index in range(1, 5)),
            attempt_event_head_sha256="5" * 64,
            detector_event_sha256="6" * 64,
            detector_status="REBASELINE",
            detector_head_sha256=None,
            attestation_disposition="NOT_REQUIRED",
            attestation_record_sha256=None,
            attestation_record_head_sha256=None,
            attestation_sha256=None,
            prepared_sha256="7" * 64,
            acknowledgement_head_sha256="8" * 64,
            finalization_event_head_sha256="9" * 64,
            telemetry_record_count=400,
            telemetry_record_head_sha256="a" * 64,
        )
        post = build_gate_c_canonical_state(
            next_window_sequence=1,
            acknowledgement_count=200,
            acknowledgement_head_sha256="8" * 64,
            attempt_count=4,
            attempt_event_count=8,
            attempt_event_head_sha256="5" * 64,
            detector_event_count=1,
            detector_event_head_sha256="6" * 64,
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=1,
            finalization_event_count=5,
            finalization_event_head_sha256="9" * 64,
            telemetry_record_count=400,
            telemetry_record_head_sha256="a" * 64,
        )
        result = build_gate_c_checkpoint_result_v3(
            envelope=envelope,
            pre_state=pre,
            post_state=post,
            processed_window_sequences=(0,),
            checkpoint_effects=(effect,),
        )
        self.assertEqual(
            verify_gate_c_checkpoint_result_v3(result, envelope=envelope), result
        )
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_checkpoint_result_v2(result, envelope=object())

    def test_bound_has_exact_two_field_authority_and_derived_values(self) -> None:
        bound = GateCWindowExecutionBound(3, 2)
        self.assertEqual(bound.allowed_window_sequences, (3, 4))
        self.assertEqual(bound.expected_next_window_sequence, 5)
        self.assertEqual(set(bound.__dataclass_fields__), {"start_window_sequence", "window_count"})

    def test_bound_rejects_bool_float_string_zero_negative_and_forgery(self) -> None:
        for start, count in ((False, 1), (0, True), (0.0, 1), (0, "1"), (-1, 1), (0, 0), (0, -1)):
            with self.assertRaises(GateCWindowExecutionError):
                GateCWindowExecutionBound(start, count)
        forged = object.__new__(GateCWindowExecutionBound)
        object.__setattr__(forged, "start_window_sequence", False)
        object.__setattr__(forged, "window_count", 1)
        with self.assertRaises(GateCWindowExecutionError):
            verify_gate_c_window_execution_bound(forged)

    def test_envelope_round_trips_by_full_reconstruction(self) -> None:
        plan, campaign, head, sources, envelope = _fixture()
        document = gate_c_bounded_execution_envelope_document(envelope)
        verified = verify_gate_c_bounded_execution_envelope(
            document, plan=plan, campaign_binding=campaign,
            source_head=head, sources=sources,
        )
        self.assertEqual(verified, envelope)

    def test_v1_envelope_golden_digest_is_frozen(self) -> None:
        *_unused, envelope = _fixture()
        self.assertEqual(
            envelope.envelope_sha256,
            "cf601041c7e0462732a3c05d5319b24f57dcc72a9c70c047c6274220ee930d7e",
        )

    def test_v2_envelope_binds_both_revisions_and_round_trips(self) -> None:
        plan, campaign, head, sources, envelope = _fixture_v2()
        document = gate_c_bounded_execution_envelope_document_v2(envelope)
        verified = verify_gate_c_bounded_execution_envelope_v2(
            document,
            plan=plan,
            campaign_binding=campaign,
            source_head=head,
            sources=sources,
            execution_source_revision="b" * 40,
        )
        self.assertEqual(verified, envelope)
        payload = document["envelope_payload"]
        self.assertEqual(payload["source_revision"], "revision")
        self.assertEqual(payload["execution_source_revision"], "b" * 40)

        *_unused, changed = _fixture_v2(execution_source_revision="c" * 40)
        self.assertNotEqual(changed.envelope_sha256, envelope.envelope_sha256)

    def test_v1_and_v2_envelopes_never_cross_deserialize(self) -> None:
        legacy_plan, legacy_campaign, legacy_head, legacy_sources, legacy = _fixture()
        current_plan, current_campaign, current_head, current_sources, current = _fixture_v2()
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_bounded_execution_envelope_v2(
                gate_c_bounded_execution_envelope_document(legacy),
                plan=current_plan,
                campaign_binding=current_campaign,
                source_head=current_head,
                sources=current_sources,
                execution_source_revision="b" * 40,
            )
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_bounded_execution_envelope(
                gate_c_bounded_execution_envelope_document_v2(current),
                plan=legacy_plan,
                campaign_binding=legacy_campaign,
                source_head=legacy_head,
                sources=legacy_sources,
            )

    def test_v2_execution_revision_is_exact_commit_identity(self) -> None:
        plan, campaign, head, sources, _envelope = _fixture_v2()
        for invalid in ("revision", "B" * 40, "b" * 39, True, 1):
            with self.subTest(invalid=invalid), self.assertRaises(
                GateCBoundedExecutionError
            ):
                build_gate_c_bounded_execution_envelope_v2(
                    plan=plan,
                    campaign_binding=campaign,
                    source_head=head,
                    sources=sources,
                    execution_bound=GateCWindowExecutionBound(0, 1),
                    execution_source_revision=invalid,
                )

        *_unused, envelope = _fixture_v2()
        forged = object.__new__(type(envelope))
        for field in fields(type(envelope)):
            object.__setattr__(forged, field.name, getattr(envelope, field.name))
        object.__setattr__(forged, "execution_source_revision", False)
        with self.assertRaises(GateCBoundedExecutionError):
            gate_c_bounded_execution_envelope_document_v2(forged)

    def test_v2_upstream_and_execution_revisions_cannot_be_swapped(self) -> None:
        plan, campaign, head, sources, envelope = _fixture_v2(
            source_revision="a" * 40,
            execution_source_revision="b" * 40,
        )
        document = gate_c_bounded_execution_envelope_document_v2(envelope)
        payload = document["envelope_payload"]
        payload["source_revision"], payload["execution_source_revision"] = (
            payload["execution_source_revision"],
            payload["source_revision"],
        )
        document["envelope_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V2\x00",
            payload,
        )
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_bounded_execution_envelope_v2(
                document,
                plan=plan,
                campaign_binding=campaign,
                source_head=head,
                sources=sources,
                execution_source_revision="b" * 40,
            )

    def test_every_bound_and_identity_mutation_changes_or_invalidates_digest(self) -> None:
        plan, campaign, head, sources, envelope = _fixture()
        original = envelope.envelope_sha256
        for start, count in ((0, 2), (1, 1)):
            changed_plan = copy.deepcopy(plan)
            changed_plan["observed"]["next_window_sequence"] = start
            changed_plan.pop("plan_sha256")
            changed_plan["plan_sha256"] = strict_canonical_digest(
                b"VD::EXP012_SCALE_GATE_C_PLAN::V1\x00", changed_plan
            )
            changed = build_gate_c_bounded_execution_envelope(
                plan=changed_plan, campaign_binding=campaign, source_head=head,
                sources=sources, execution_bound=GateCWindowExecutionBound(start, count),
            )
            self.assertNotEqual(changed.envelope_sha256, original)
        for field in (
            "campaign_binding_sha256", "gate_a_evidence_sha256",
            "source_store_binding_sha256", "source_head_sha256",
            "outbox_head_sha256", "source_revision", "producer_run_id",
            "configuration_identity", "data_identity", "flat_binding_id",
            "hnsw_binding_id", "environment_manifest_sha256",
        ):
            document = gate_c_bounded_execution_envelope_document(envelope)
            payload = document["envelope_payload"]
            payload[field] = ("f" * 64 if field.endswith("sha256") else "changed")
            document["envelope_sha256"] = strict_canonical_digest(
                b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V1\x00",
                payload,
            )
            with self.assertRaises(GateCBoundedExecutionError):
                verify_gate_c_bounded_execution_envelope(
                    document, plan=plan, campaign_binding=campaign,
                    source_head=head, sources=sources,
                )

    def test_derived_allowed_or_postcondition_tamper_fails(self) -> None:
        plan, campaign, head, sources, envelope = _fixture()
        for name, value in (
            ("allowed_window_sequences", [1]),
            ("expected_next_window_sequence", 2),
        ):
            document = gate_c_bounded_execution_envelope_document(envelope)
            document["envelope_payload"]["execution_bound"][name] = value
            document["envelope_sha256"] = strict_canonical_digest(
                b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V1\x00",
                document["envelope_payload"],
            )
            with self.assertRaises(GateCBoundedExecutionError):
                verify_gate_c_bounded_execution_envelope(
                    document, plan=plan, campaign_binding=campaign,
                    source_head=head, sources=sources,
                )

    def test_oversized_untrusted_bound_refuses_before_range_materialization(self) -> None:
        plan, campaign, head, sources, envelope = _fixture()
        document = gate_c_bounded_execution_envelope_document(envelope)
        bound = document["envelope_payload"]["execution_bound"]
        bound["window_count"] = 10**100
        bound["expected_next_window_sequence"] = 10**100
        document["envelope_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V1\x00",
            document["envelope_payload"],
        )
        with self.assertRaises(GateCBoundedExecutionError) as raised:
            verify_gate_c_bounded_execution_envelope(
                document,
                plan=plan,
                campaign_binding=campaign,
                source_head=head,
                sources=sources,
            )
        self.assertEqual(raised.exception.code, "GATE_C_BOUNDED_ENVELOPE_INVALID")

    def test_forged_envelope_limits_precede_allowed_range_materialization(self) -> None:
        *_unused, envelope = _fixture()
        maximum = envelope.scale_contract.expected_windows
        invalid_bounds = (
            GateCWindowExecutionBound(0, maximum + 1),
            GateCWindowExecutionBound(maximum - 1, 2),
            GateCWindowExecutionBound(0, 10**100),
            GateCWindowExecutionBound(maximum, 1),
        )
        for bound in invalid_bounds:
            forged = _forged_envelope_with_bound(envelope, bound)
            with self.subTest(bound=bound), mock.patch.object(
                GateCWindowExecutionBound,
                "allowed_window_sequences",
                new_callable=mock.PropertyMock,
                side_effect=AssertionError(
                    "range materialized before campaign validation"
                ),
            ) as allowed, self.assertRaises(GateCBoundedExecutionError) as raised:
                gate_c_bounded_execution_envelope_document(forged)
            self.assertEqual(
                raised.exception.code, "GATE_C_BOUNDED_ENVELOPE_INVALID"
            )
            allowed.assert_not_called()

    def test_forged_envelope_accepts_exact_upper_campaign_edge(self) -> None:
        *_unused, envelope = _fixture(
            start=11,
            count=1,
            target_profile=Exp012ScaleProfile.SCALE_2400,
        )
        payload = gate_c_bounded_execution_envelope_payload(envelope)
        self.assertEqual(
            payload["execution_bound"]["allowed_window_sequences"],
            [11],
        )

    def test_producer_mixed_malformed_and_sequence_drift_fail(self) -> None:
        plan, campaign, head, sources, _ = _fixture()
        for replacement in (
            "logsim-v2:" + "b" * 32 + ":1",
            "logsim-v2:" + "a" * 32 + ":2",
            "invented:1",
        ):
            changed = list(sources)
            object.__setattr__(changed[1], "query_id", replacement)
            with self.assertRaises(GateCBoundedExecutionError):
                build_gate_c_bounded_execution_envelope(
                    plan=plan, campaign_binding=campaign, source_head=head,
                    sources=tuple(changed), execution_bound=GateCWindowExecutionBound(0, 1),
                )
            object.__setattr__(changed[1], "query_id", "logsim-v2:" + "a" * 32 + ":1")

    def test_source_identity_and_store_head_forgery_fail(self) -> None:
        plan, campaign, head, sources, _ = _fixture()
        changed_sources = list(sources)
        original_stream = changed_sources[10].stream_key
        object.__setattr__(
            changed_sources[10],
            "stream_key",
            MonitorStreamKey(
                "stream", Metric.L2, "target-075", "different-cfg",
                "data", "flat", "hnsw",
            ),
        )
        with self.assertRaises(GateCBoundedExecutionError) as identity:
            build_gate_c_bounded_execution_envelope(
                plan=plan, campaign_binding=campaign, source_head=head,
                sources=tuple(changed_sources), execution_bound=GateCWindowExecutionBound(0, 1),
            )
        self.assertEqual(identity.exception.code, "GATE_C_BOUNDED_SOURCE_IDENTITY_MISMATCH")
        object.__setattr__(changed_sources[10], "stream_key", original_stream)

        forged_head = replace(head, source_count=False)
        with self.assertRaises(GateCBoundedExecutionError):
            build_gate_c_bounded_execution_envelope(
                plan=plan, campaign_binding=campaign, source_head=forged_head,
                sources=sources, execution_bound=GateCWindowExecutionBound(0, 1),
            )

    def test_checkpoint_result_is_distinct_and_exact(self) -> None:
        *_unused, envelope = _fixture()
        pre = build_gate_c_canonical_state(
            next_window_sequence=0,
            acknowledgement_count=0,
            acknowledgement_head_sha256=None,
            attempt_count=0,
            attempt_event_count=0,
            attempt_event_head_sha256=None,
            detector_event_count=0,
            detector_event_head_sha256=None,
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=0,
            finalization_event_count=0,
            finalization_event_head_sha256=None,
            telemetry_record_count=0,
            telemetry_record_head_sha256=None,
        )
        effect = build_gate_c_window_checkpoint_effect(
            window_sequence=0,
            source_window_sha256="1" * 64,
            attempt_sha256s=tuple(f"{index:064x}" for index in range(1, 5)),
            attempt_event_head_sha256="5" * 64,
            detector_event_sha256="6" * 64,
            detector_status="REBASELINE",
            detector_head_sha256=None,
            attestation_disposition="NOT_REQUIRED",
            attestation_record_sha256=None,
            attestation_record_head_sha256=None,
            attestation_sha256=None,
            prepared_sha256="7" * 64,
            acknowledgement_head_sha256="8" * 64,
            finalization_event_head_sha256="9" * 64,
            telemetry_record_count=400,
            telemetry_record_head_sha256="a" * 64,
        )
        post = build_gate_c_canonical_state(
            next_window_sequence=1,
            acknowledgement_count=200,
            acknowledgement_head_sha256="8" * 64,
            attempt_count=4,
            attempt_event_count=8,
            attempt_event_head_sha256="5" * 64,
            detector_event_count=1,
            detector_event_head_sha256="6" * 64,
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=1,
            finalization_event_count=5,
            finalization_event_head_sha256="9" * 64,
            telemetry_record_count=400,
            telemetry_record_head_sha256="a" * 64,
        )
        result = build_gate_c_checkpoint_result(
            envelope=envelope,
            pre_state=pre,
            post_state=post,
            processed_window_sequences=(0,),
            checkpoint_effects=(effect,),
        )
        self.assertEqual(
            verify_gate_c_checkpoint_result(result, envelope=envelope), result
        )
        self.assertEqual(result["checkpoint_result_payload"]["full_campaign_complete"], False)
        self.assertNotIn("result_payload", result)
        counts = result["checkpoint_result_payload"]["checkpoint_counts"]
        self.assertEqual(counts["acknowledgement"], {"pre": 0, "post": 200, "delta": 200})
        self.assertEqual(counts["attempt"], {"pre": 0, "post": 4, "delta": 4})
        self.assertEqual(counts["telemetry_record"], {"pre": 0, "post": 400, "delta": 400})

        for section, field in (
            ("post_state", "acknowledgement_head_sha256"),
            ("post_state", "attempt_event_head_sha256"),
            ("post_state", "detector_event_head_sha256"),
            ("post_state", "finalization_event_head_sha256"),
            ("post_state", "telemetry_record_head_sha256"),
            ("checkpoint_effects", "detector_event_sha256"),
            ("checkpoint_effects", "prepared_sha256"),
        ):
            tampered = copy.deepcopy(result)
            if section == "checkpoint_effects":
                tampered["checkpoint_result_payload"][section][0]["effect_payload"][field] = "f" * 64
            else:
                tampered["checkpoint_result_payload"][section]["state_payload"][field] = "f" * 64
            with self.subTest(section=section, field=field), self.assertRaises(
                GateCBoundedExecutionError
            ):
                verify_gate_c_checkpoint_result(tampered, envelope=envelope)

        malformed_count = copy.deepcopy(result)
        malformed_effect = malformed_count["checkpoint_result_payload"][
            "checkpoint_effects"
        ][0]
        malformed_effect["effect_payload"]["telemetry_record_count"] = 1
        malformed_effect["effect_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_WINDOW_EFFECT::V1\x00",
            malformed_effect["effect_payload"],
        )
        malformed_count["checkpoint_result_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V1\x00",
            malformed_count["checkpoint_result_payload"],
        )
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_checkpoint_result(malformed_count, envelope=envelope)

    def test_checkpoint_effect_requires_exact_governed_telemetry_count(self) -> None:
        common = {
            "window_sequence": 0,
            "source_window_sha256": "1" * 64,
            "attempt_sha256s": tuple(f"{index:064x}" for index in range(1, 5)),
            "attempt_event_head_sha256": "5" * 64,
            "detector_event_sha256": "6" * 64,
            "detector_status": "REBASELINE",
            "detector_head_sha256": None,
            "attestation_disposition": "NOT_REQUIRED",
            "attestation_record_sha256": None,
            "attestation_record_head_sha256": None,
            "attestation_sha256": None,
            "prepared_sha256": "7" * 64,
            "acknowledgement_head_sha256": "8" * 64,
            "finalization_event_head_sha256": "9" * 64,
            "telemetry_record_head_sha256": "a" * 64,
        }
        for count in (-1, 0, 1, 399, 401):
            with self.subTest(count=count), self.assertRaises(
                GateCBoundedExecutionError
            ):
                build_gate_c_window_checkpoint_effect(
                    **common, telemetry_record_count=count
                )
        effect = build_gate_c_window_checkpoint_effect(
            **common, telemetry_record_count=400
        )
        self.assertEqual(effect["effect_payload"]["telemetry_record_count"], 400)

    def test_two_window_effect_counts_are_individually_and_aggregately_exact(self) -> None:
        *_unused, envelope = _fixture(count=2)
        pre = build_gate_c_canonical_state(
            next_window_sequence=0,
            acknowledgement_count=0,
            acknowledgement_head_sha256=None,
            attempt_count=0,
            attempt_event_count=0,
            attempt_event_head_sha256=None,
            detector_event_count=0,
            detector_event_head_sha256=None,
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=0,
            finalization_event_count=0,
            finalization_event_head_sha256=None,
            telemetry_record_count=0,
            telemetry_record_head_sha256=None,
        )
        effects = tuple(
            build_gate_c_window_checkpoint_effect(
                window_sequence=window,
                source_window_sha256=f"{window + 1:064x}",
                attempt_sha256s=tuple(
                    f"{window * 4 + index + 10:064x}" for index in range(4)
                ),
                attempt_event_head_sha256=f"{window + 20:064x}",
                detector_event_sha256=f"{window + 30:064x}",
                detector_status="REBASELINE",
                detector_head_sha256=None,
                attestation_disposition="NOT_REQUIRED",
                attestation_record_sha256=None,
                attestation_record_head_sha256=None,
                attestation_sha256=None,
                prepared_sha256=f"{window + 40:064x}",
                acknowledgement_head_sha256=f"{window + 50:064x}",
                finalization_event_head_sha256=f"{window + 60:064x}",
                telemetry_record_count=400,
                telemetry_record_head_sha256=f"{window + 70:064x}",
            )
            for window in range(2)
        )
        last = effects[-1]["effect_payload"]
        post = build_gate_c_canonical_state(
            next_window_sequence=2,
            acknowledgement_count=400,
            acknowledgement_head_sha256=last["acknowledgement_head_sha256"],
            attempt_count=8,
            attempt_event_count=16,
            attempt_event_head_sha256=last["attempt_event_head_sha256"],
            detector_event_count=2,
            detector_event_head_sha256=last["detector_event_sha256"],
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=2,
            finalization_event_count=10,
            finalization_event_head_sha256=last["finalization_event_head_sha256"],
            telemetry_record_count=800,
            telemetry_record_head_sha256=last["telemetry_record_head_sha256"],
        )
        result = build_gate_c_checkpoint_result(
            envelope=envelope,
            pre_state=pre,
            post_state=post,
            processed_window_sequences=(0, 1),
            checkpoint_effects=effects,
        )
        self.assertEqual(
            verify_gate_c_checkpoint_result(result, envelope=envelope), result
        )

        malformed = copy.deepcopy(result)
        for item, count in zip(
            malformed["checkpoint_result_payload"]["checkpoint_effects"],
            (399, 401),
            strict=True,
        ):
            item["effect_payload"]["telemetry_record_count"] = count
            item["effect_sha256"] = strict_canonical_digest(
                b"VD::EXP012_SCALE_GATE_C_WINDOW_EFFECT::V1\x00",
                item["effect_payload"],
            )
        malformed["checkpoint_result_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V1\x00",
            malformed["checkpoint_result_payload"],
        )
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_checkpoint_result(malformed, envelope=envelope)

        wrong_total = copy.deepcopy(post)
        wrong_total["state_payload"]["telemetry_record_count"] = 799
        wrong_total["state_sha256"] = strict_canonical_digest(
            b"VD::EXP012_SCALE_GATE_C_CANONICAL_STATE::V1\x00",
            wrong_total["state_payload"],
        )
        with self.assertRaises(GateCBoundedExecutionError):
            build_gate_c_checkpoint_result(
                envelope=envelope,
                pre_state=pre,
                post_state=wrong_total,
                processed_window_sequences=(0, 1),
                checkpoint_effects=effects,
            )

    def test_evaluated_checkpoint_binds_attestation_and_requires_complete_effect(self) -> None:
        *_unused, envelope = _fixture(
            start=1,
            count=1,
            target_profile=Exp012ScaleProfile.SCALE_10000,
        )
        pre = build_gate_c_canonical_state(
            next_window_sequence=1,
            acknowledgement_count=200,
            acknowledgement_head_sha256="1" * 64,
            attempt_count=4,
            attempt_event_count=8,
            attempt_event_head_sha256="2" * 64,
            detector_event_count=1,
            detector_event_head_sha256="3" * 64,
            attestation_record_count=0,
            attestation_record_head_sha256=None,
            finalization_window_count=1,
            finalization_event_count=5,
            finalization_event_head_sha256="4" * 64,
            telemetry_record_count=400,
            telemetry_record_head_sha256="5" * 64,
        )
        effect = build_gate_c_window_checkpoint_effect(
            window_sequence=1,
            source_window_sha256="6" * 64,
            attempt_sha256s=tuple(f"{index:064x}" for index in range(11, 15)),
            attempt_event_head_sha256="7" * 64,
            detector_event_sha256="8" * 64,
            detector_status="EVALUATED",
            detector_head_sha256="9" * 64,
            attestation_disposition="COMMITTED",
            attestation_record_sha256="a" * 64,
            attestation_record_head_sha256="c" * 64,
            attestation_sha256="b" * 64,
            prepared_sha256="c" * 64,
            acknowledgement_head_sha256="d" * 64,
            finalization_event_head_sha256="e" * 64,
            telemetry_record_count=400,
            telemetry_record_head_sha256="f" * 64,
        )
        post = build_gate_c_canonical_state(
            next_window_sequence=2,
            acknowledgement_count=400,
            acknowledgement_head_sha256="d" * 64,
            attempt_count=8,
            attempt_event_count=16,
            attempt_event_head_sha256="7" * 64,
            detector_event_count=2,
            detector_event_head_sha256="8" * 64,
            attestation_record_count=1,
            attestation_record_head_sha256="c" * 64,
            finalization_window_count=2,
            finalization_event_count=10,
            finalization_event_head_sha256="e" * 64,
            telemetry_record_count=800,
            telemetry_record_head_sha256="f" * 64,
        )
        result = build_gate_c_checkpoint_result(
            envelope=envelope,
            pre_state=pre,
            post_state=post,
            processed_window_sequences=(1,),
            checkpoint_effects=(effect,),
        )
        self.assertEqual(
            verify_gate_c_checkpoint_result(result, envelope=envelope), result
        )

        import copy

        for section, field in (
            ("post_state", "attestation_record_head_sha256"),
            ("checkpoint_effects", "attestation_record_sha256"),
            ("checkpoint_effects", "attestation_record_head_sha256"),
            ("checkpoint_effects", "detector_event_sha256"),
            ("checkpoint_effects", "finalization_event_head_sha256"),
        ):
            tampered = copy.deepcopy(result)
            target = (
                tampered["checkpoint_result_payload"][section]["state_payload"]
                if section == "post_state"
                else tampered["checkpoint_result_payload"][section][0]["effect_payload"]
            )
            target[field] = "0" * 64
            with self.subTest(section=section, field=field), self.assertRaises(
                GateCBoundedExecutionError
            ):
                verify_gate_c_checkpoint_result(tampered, envelope=envelope)

        missing_effect = copy.deepcopy(result)
        missing_effect["checkpoint_result_payload"]["checkpoint_effects"] = []
        with self.assertRaises(GateCBoundedExecutionError):
            verify_gate_c_checkpoint_result(missing_effect, envelope=envelope)

        for field in (
            "detector_event_sha256",
            "attestation_record_sha256",
            "attestation_record_head_sha256",
            "finalization_event_head_sha256",
        ):
            missing_field = copy.deepcopy(result)
            del missing_field["checkpoint_result_payload"]["checkpoint_effects"][0][
                "effect_payload"
            ][field]
            with self.subTest(missing=field), self.assertRaises(
                GateCBoundedExecutionError
            ):
                verify_gate_c_checkpoint_result(missing_field, envelope=envelope)


if __name__ == "__main__":
    unittest.main()
