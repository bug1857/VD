from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from vdbench.config import Metric
from vdbench.exp010_gate_b_operator import (
    Exp010GateBOperatorError,
    _require_campaign_namespace as require_gate_b_namespace,
)
from vdbench.exp010_gate_c_operator import (
    Exp010GateCOperands,
    Exp010GateCOperatorError,
    _require_campaign_namespace as require_gate_c_namespace,
)
from vdbench.exp012_scale_campaign import (
    Exp012ScaleCampaignError,
    load_scale_campaign_marker,
    marker_path,
    write_scale_campaign_marker,
)
from vdbench.exp012_scale_contract import Exp012ScaleProfile, build_exp012_scale_contract


def _gate_c(root: Path) -> Exp010GateCOperands:
    return Exp010GateCOperands(
        stream_id="stream", metric=Metric.L2, threshold_stratum="target-075",
        threshold_radius=1.0, range_filter=0.0, limit=100, served_ef=400,
        dimensions=128, consistency_level="Strong", configuration_identity="cfg",
        flat_binding_id="flat", hnsw_binding_id="hnsw", source_revision="revision",
        environment_manifest_sha256="a" * 64, detector_seed=1,
        milvus_uri="http://127.0.0.1:19530", flat_collection_name="flat",
        hnsw_collection_name="hnsw", store_root=root / "stores",
        dataset001_dir=root / "dataset", exp010_output_dir=root / "output",
        etcd_container="etcd", minio_container="minio",
    )


class Exp012ScaleCampaignTests(unittest.TestCase):
    def test_marker_is_private_canonical_idempotent_and_profile_bound(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400)
            path = write_scale_campaign_marker(
                root, contract, gate_a_evidence_sha256="a" * 64
            )
            self.assertEqual(path, marker_path(root))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            before = path.read_bytes()
            binding = load_scale_campaign_marker(root)
            self.assertEqual(binding.contract, contract)
            self.assertEqual(binding.gate_a_evidence_sha256, "a" * 64)
            write_scale_campaign_marker(
                root, contract, gate_a_evidence_sha256="a" * 64
            )
            self.assertEqual(path.read_bytes(), before)
            with self.assertRaises(Exp012ScaleCampaignError):
                load_scale_campaign_marker(
                    root,
                    expected_contract=build_exp012_scale_contract(
                        Exp012ScaleProfile.SCALE_10000
                    ),
                )
            with self.assertRaises(Exp012ScaleCampaignError):
                load_scale_campaign_marker(
                    root, expected_gate_a_evidence_sha256="b" * 64
                )

    def test_marker_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path = write_scale_campaign_marker(
                root,
                build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400),
                gate_a_evidence_sha256="a" * 64,
            )
            path.write_bytes(b"{}")
            with self.assertRaises(Exp012ScaleCampaignError):
                load_scale_campaign_marker(root)

    def test_legacy_gate_b_refuses_marked_scale_campaign(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400)
            write_scale_campaign_marker(
                root, contract, gate_a_evidence_sha256="a" * 64
            )
            with self.assertRaises(Exp010GateBOperatorError):
                require_gate_b_namespace(root, accepted_scale_contract_sha256=None)
            require_gate_b_namespace(
                root, accepted_scale_contract_sha256=contract.contract_sha256
            )

    def test_legacy_gate_c_refuses_marked_scale_campaign(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_10000)
            write_scale_campaign_marker(
                root, contract, gate_a_evidence_sha256="a" * 64
            )
            operands = _gate_c(root)
            with self.assertRaises(Exp010GateCOperatorError):
                require_gate_c_namespace(
                    operands, accepted_scale_contract_sha256=None
                )
            require_gate_c_namespace(
                operands,
                accepted_scale_contract_sha256=contract.contract_sha256,
            )

    def test_scale_gate_c_requires_marker(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            contract = build_exp012_scale_contract(Exp012ScaleProfile.SCALE_2400)
            with self.assertRaises(Exp010GateCOperatorError) as raised:
                require_gate_c_namespace(
                    _gate_c(Path(raw)),
                    accepted_scale_contract_sha256=contract.contract_sha256,
                )
            self.assertEqual(
                raised.exception.code, "GATE_C_EXP012_CAMPAIGN_MARKER_MISSING"
            )


if __name__ == "__main__":
    unittest.main()
