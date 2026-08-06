"""TDD coverage for SearchConfiguration.validate()'s root type/contract gate.

This is the root hardening this file exists to prove: no serializer,
producer, evaluator, or database adapter downstream of SearchConfiguration
should ever be the first layer to discover an invalid runtime type -- that
must happen here, in validate() itself.
"""

from __future__ import annotations

import unittest

from vdbench.config import (
    ContractViolation,
    IndexTrack,
    Metric,
    SearchConfiguration,
    THRESHOLD_LABELS,
)


def _config(**overrides: object) -> SearchConfiguration:
    fields: dict[str, object] = dict(
        metric=Metric.COSINE,
        threshold_label="target-025",
        radius=0.2,
        index_track=IndexTrack.HNSW,
        ef=800,
        limit=100,
        consistency_level="Strong",
    )
    fields.update(overrides)
    return SearchConfiguration(**fields)


class SearchConfigurationValidateRootHardeningTests(unittest.TestCase):
    # -- radius --------------------------------------------------------

    def test_radius_false_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius=False).validate()

    def test_radius_true_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius=True).validate()

    def test_radius_string_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius="0.5").validate()

    def test_radius_none_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius=None).validate()

    def test_radius_nan_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius=float("nan")).validate()

    def test_radius_positive_infinity_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius=float("inf")).validate()

    def test_radius_negative_infinity_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(radius=float("-inf")).validate()

    def test_valid_python_float_radius_is_accepted(self) -> None:
        _config(radius=0.2).validate()  # must not raise

    def test_l2_and_cosine_bounds_unchanged(self) -> None:
        _config(metric=Metric.L2, threshold_label="target-075", radius=0.6).validate()
        with self.assertRaises(ContractViolation):
            _config(metric=Metric.L2, threshold_label="target-075", radius=0.0).validate()
        _config(radius=-0.999).validate()  # COSINE lower bound inclusive
        with self.assertRaises(ContractViolation):
            _config(radius=1.0).validate()  # COSINE upper bound exclusive
        with self.assertRaises(ContractViolation):
            _config(radius=-1.0001).validate()

    # -- limit -----------------------------------------------------------

    def test_limit_float_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(limit=100.0).validate()

    def test_limit_bool_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(limit=True).validate()

    def test_limit_string_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(limit="100").validate()

    def test_limit_wrong_int_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(limit=99).validate()

    def test_limit_correct_int_is_accepted(self) -> None:
        _config(limit=100).validate()  # must not raise

    # -- ef ----------------------------------------------------------------

    def test_ef_float_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(ef=800.0).validate()

    def test_ef_bool_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(ef=True).validate()

    def test_flat_still_requires_ef_none(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(index_track=IndexTrack.FLAT, ef=800).validate()
        _config(index_track=IndexTrack.FLAT, ef=None).validate()  # must not raise

    # -- metric / index_track / consistency_level / threshold_label --------

    def test_plain_string_metric_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(metric="COSINE").validate()

    def test_plain_string_index_track_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(index_track="HNSW").validate()

    def test_unknown_threshold_label_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(threshold_label="not-a-real-label").validate()

    def test_every_registered_threshold_label_is_accepted(self) -> None:
        for label in THRESHOLD_LABELS:
            with self.subTest(label=label):
                _config(threshold_label=label).validate()  # must not raise

    def test_consistency_level_non_string_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(consistency_level=123).validate()

    def test_consistency_level_wrong_string_is_rejected(self) -> None:
        with self.assertRaises(ContractViolation):
            _config(consistency_level="Eventually").validate()

    # -- stable messages, ContractViolation type ----------------------------

    def test_invalid_inputs_raise_contract_violation_with_stable_messages(self) -> None:
        cases = (
            (dict(radius=False), "radius must be a real number"),
            (dict(radius=float("nan")), "radius must be finite"),
            (dict(limit=100.0), "limit must be an integer"),
            (dict(limit=99), "limit must equal 100"),
            (dict(ef=800.0), "HNSW ef must be an integer"),
            (dict(metric="COSINE"), "metric must be a Metric enum member"),
            (dict(index_track="HNSW"), "index_track must be an IndexTrack enum member"),
            (dict(threshold_label="bogus"), "threshold_label must be one of"),
        )
        for overrides, expected_substring in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ContractViolation) as ctx:
                    _config(**overrides).validate()
                self.assertIn(expected_substring, str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
