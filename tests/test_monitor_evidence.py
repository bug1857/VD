"""Red-first coverage for strict restart-durable monitor evidence."""

from __future__ import annotations

import copy
import unittest

from vdbench.config import Metric
from vdbench.drift import (
    Signal,
    SignalEvidence,
    build_evidence_provenance,
    finalize_window_evidence,
)
from vdbench.monitor_evidence import (
    MonitorEvidenceCodecError,
    decode_persisted_window_evidence,
    encode_persisted_window_evidence,
)


def _evidence():
    provenance = build_evidence_provenance(
        metric=Metric.L2,
        threshold_stratum="target-075",
        reference_window_id="reference",
        current_window_id="current-1",
        reference_manifest_sha256="a" * 64,
        current_manifest_sha256="b" * 64,
        configuration_identity="configuration-v1",
        data_identity="dataset-v1",
        flat_binding_id="flat-v1",
        hnsw_binding_id="hnsw-v1",
        reference_audit_ids=tuple(range(50)),
        reference_audit_rank_digests=tuple("c" * 64 for _ in range(50)),
        current_audit_ids=tuple(range(50)),
        current_audit_rank_digests=tuple("d" * 64 for _ in range(50)),
    )
    counts = {
        Signal.QUERY_VECTOR: 200,
        Signal.THRESHOLD: 200,
        Signal.CARDINALITY: 50,
        Signal.RECALL: 50,
    }
    floors = {
        Signal.QUERY_VECTOR: 0.01,
        Signal.THRESHOLD: 0.20,
        Signal.CARDINALITY: 0.20,
        Signal.RECALL: 0.02,
    }
    return finalize_window_evidence(
        metric=Metric.L2,
        window_id="current-1",
        provenance=provenance,
        signals=tuple(
            SignalEvidence(
                signal=signal,
                complete=True,
                reference_count=counts[signal],
                current_count=counts[signal],
                statistic=0.0,
                effect=0.0,
                effect_floor=floors[signal],
                raw_p_value=1.0,
            )
            for signal in Signal
        ),
    )


class MonitorEvidenceCodecTests(unittest.TestCase):
    def test_canonical_round_trip_preserves_complete_evidence_exactly(self) -> None:
        evidence = _evidence()

        document = encode_persisted_window_evidence(evidence)
        restored = decode_persisted_window_evidence(document)

        self.assertEqual(restored, evidence)

    def test_tampered_payload_fails_closed_on_checksum_mismatch(self) -> None:
        document = copy.deepcopy(encode_persisted_window_evidence(_evidence()))
        document["payload"]["signals"][0]["effect"] = 0.5

        with self.assertRaisesRegex(MonitorEvidenceCodecError, "SHA256_MISMATCH"):
            decode_persisted_window_evidence(document)


if __name__ == "__main__":
    unittest.main()
