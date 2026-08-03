"""Pure EXP-005 extraction from assembled shadow traces to detector evidence.

This boundary validates two independently assembled 200-query windows, selects
their deterministic 50-query audit subsets, and delegates all statistics to
``vdbench.drift``.  It never queries Milvus, evaluates policy, or actuates.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from numbers import Integral

from .config import IndexTrack, Metric
from .drift import (
    AUDIT_QUERY_COUNT,
    ELIGIBLE_QUERY_COUNT,
    RESULT_LIMIT,
    AuditSelection,
    RecallAuditSample,
    Signal,
    SignalEvidence,
    WindowEvidence,
    canonical_serialize_tuple,
    finalize_window_evidence,
    ks_signal_test,
    query_vector_signal_test,
    recall_signal_test,
    select_audit_sample,
)
from .milvus_actuation import (
    ShadowAuditTrace,
    ShadowIdentityEvidence,
    ShadowQueryAuditTrace,
)
from .shadow_window import AssembledShadowWindow, PersistedShadowTraceEnvelope


_IDENTITY_FIELDS = (
    ("data_identity", "DATA_IDENTITY"),
    ("configuration_identity", "CONFIGURATION_IDENTITY"),
    ("flat_binding_id", "FLAT_BINDING"),
    ("hnsw_binding_id", "HNSW_BINDING"),
)


def _append(reasons: list[str], code: str) -> None:
    if code not in reasons:
        reasons.append(code)


def _fallback_metric(current: object, requested: object) -> Metric:
    if isinstance(requested, Metric):
        return requested
    if isinstance(current, AssembledShadowWindow) and isinstance(current.metric, Metric):
        return current.metric
    return Metric.L2


def _fallback_window_id(current: object) -> int | str:
    if isinstance(current, AssembledShadowWindow):
        value = current.window_id
        if not isinstance(value, bool) and isinstance(value, Integral):
            return int(value)
        if isinstance(value, str) and value:
            return value
    return "invalid-current-window"


def _incomplete(
    *, current: object, metric: object, reasons: Sequence[str]
) -> WindowEvidence:
    return WindowEvidence(
        metric=_fallback_metric(current, metric),
        window_id=_fallback_window_id(current),
        signals=(),
        complete=False,
        reason_codes=tuple(dict.fromkeys(reasons)),
    )


def _canonical_record_map(
    window: AssembledShadowWindow,
    *,
    label: str,
    reasons: list[str],
) -> dict[bytes, ShadowQueryAuditTrace] | None:
    if not isinstance(window.query_records, tuple) or len(window.query_records) != ELIGIBLE_QUERY_COUNT:
        _append(reasons, f"{label}_QUERY_RECORD_COUNT_INVALID")
        return None
    result: dict[bytes, ShadowQueryAuditTrace] = {}
    for record in window.query_records:
        if not isinstance(record, ShadowQueryAuditTrace):
            _append(reasons, f"{label}_QUERY_RECORD_INVALID")
            return None
        try:
            key = canonical_serialize_tuple((record.query_id,))
        except (TypeError, ValueError):
            _append(reasons, f"{label}_QUERY_ID_INVALID")
            return None
        if key in result:
            _append(reasons, f"{label}_QUERY_ID_DUPLICATE")
            return None
        result[key] = record
    return result


def _identity_bundle(
    window: AssembledShadowWindow,
    *,
    label: str,
    metric: Metric,
    reasons: list[str],
) -> tuple[str, str, str, str] | None:
    """Return four validated identities, requiring agreement across all traces."""

    if not isinstance(window.envelopes, tuple) or len(window.envelopes) != 4:
        _append(reasons, f"{label}_ENVELOPE_COUNT_INVALID")
        return None
    expected: tuple[str, str, str, str] | None = None
    for envelope in window.envelopes:
        trace = envelope.trace if isinstance(envelope, PersistedShadowTraceEnvelope) else None
        if not isinstance(trace, ShadowAuditTrace) or not trace.complete:
            _append(reasons, f"{label}_TRACE_INVALID")
            return None
        if trace.metric is not metric or trace.threshold_stratum != window.threshold_stratum:
            _append(reasons, f"{label}_TRACE_COMPATIBILITY_MISMATCH")
            return None
        if not isinstance(trace.flat_identity, ShadowIdentityEvidence) or trace.flat_identity.track is not IndexTrack.FLAT:
            _append(reasons, f"{label}_FLAT_BINDING_INVALID")
            return None
        if not isinstance(trace.hnsw_identity, ShadowIdentityEvidence) or trace.hnsw_identity.track is not IndexTrack.HNSW:
            _append(reasons, f"{label}_HNSW_BINDING_INVALID")
            return None
        values = (
            trace.data_identity,
            trace.configuration_identity,
            trace.flat_identity.expected_binding_id,
            trace.hnsw_identity.expected_binding_id,
        )
        if any(not isinstance(value, str) or not value for value in values):
            _append(reasons, f"{label}_IDENTITY_INVALID")
            return None
        if expected is None:
            expected = values
            continue
        for index, (_, code) in enumerate(_IDENTITY_FIELDS):
            if values[index] != expected[index]:
                _append(reasons, f"{label}_{code}_INCONSISTENT")
    return expected if not reasons else None


def _compare_identities(
    reference: tuple[str, str, str, str],
    current: tuple[str, str, str, str],
    *,
    reasons: list[str],
) -> None:
    for index, (_, code) in enumerate(_IDENTITY_FIELDS):
        if reference[index] != current[index]:
            _append(reasons, f"{code}_MISMATCH")


def _selection(
    window: AssembledShadowWindow,
    *,
    detector_seed: int,
    metric: Metric,
    label: str,
    reasons: list[str],
) -> AuditSelection | None:
    selected = select_audit_sample(
        tuple(record.query_id for record in window.query_records),
        detector_seed=detector_seed,
        metric=metric,
        window_id=window.window_id,
    )
    if not selected.complete:
        _append(reasons, f"{label}_AUDIT_SELECTION_INCOMPLETE")
        return None
    if (
        len(selected.query_ids) != AUDIT_QUERY_COUNT
        or len(selected.digest_hex) != AUDIT_QUERY_COUNT
    ):
        _append(reasons, f"{label}_AUDIT_SELECTION_COUNT_INVALID")
        return None
    return selected


def _selected_records(
    selection: AuditSelection,
    records: dict[bytes, ShadowQueryAuditTrace],
    *,
    label: str,
    reasons: list[str],
) -> tuple[ShadowQueryAuditTrace, ...] | None:
    selected: list[ShadowQueryAuditTrace] = []
    for query_id in selection.query_ids:
        try:
            key = canonical_serialize_tuple((query_id,))
        except (TypeError, ValueError):
            _append(reasons, f"{label}_SELECTED_AUDIT_ID_INVALID")
            return None
        record = records.get(key)
        if record is None:
            _append(reasons, f"{label}_SELECTED_AUDIT_ID_MISSING")
            return None
        selected.append(record)
    return tuple(selected)


def _flat_oracle_agreement(record: ShadowQueryAuditTrace) -> bool:
    """Use the recorded FLAT-stage agreement; missing evidence is disagreement."""

    stages = [stage for stage in record.stages if stage.stage == "FLAT"]
    return len(stages) == 1 and stages[0].oracle_agreement is True


def _recall_sample(
    *,
    window: AssembledShadowWindow,
    identity: tuple[str, str, str, str],
    selection: AuditSelection,
    records: Sequence[ShadowQueryAuditTrace],
    label: str,
    reasons: list[str],
) -> RecallAuditSample | None:
    values: list[float] = []
    for record in records:
        sentinel_stages = [
            stage for stage in record.stages if stage.stage == "SENTINEL_HNSW"
        ]
        if (
            len(sentinel_stages) != 1
            or not sentinel_stages[0].success
            or sentinel_stages[0].timed_out
            or sentinel_stages[0].threshold_violation_count != 0
        ):
            _append(reasons, f"{label}_SENTINEL_STAGE_INVALID")
            return None
        if record.sentinel_recall is None:
            _append(reasons, f"{label}_SENTINEL_RECALL_MISSING")
            return None
        values.append(record.sentinel_recall)
    failed = sum(
        1 for record in records for stage in record.stages if not stage.success
    )
    timeouts = sum(
        1 for record in records for stage in record.stages if stage.timed_out
    )
    violations = sum(
        stage.threshold_violation_count for record in records for stage in record.stages
    )
    return RecallAuditSample(
        window_id=window.window_id,
        metric=window.metric,
        expected_audit_ids=selection.query_ids,
        observed_audit_ids=tuple(record.query_id for record in records),
        values=tuple(values),
        flat_oracle_agreement=tuple(_flat_oracle_agreement(record) for record in records),
        collection_data_identity=identity[0],
        index_build_identity=identity[1],
        sentinel_ef=100,
        limit=RESULT_LIMIT,
        failed_query_count=failed,
        timeout_query_count=timeouts,
        threshold_violation_count=violations,
    )


def _signal_failure(
    signal: Signal,
    *,
    reference_count: int,
    current_count: int,
    reason: str,
) -> SignalEvidence:
    floors = {
        Signal.QUERY_VECTOR: 0.01,
        Signal.THRESHOLD: 0.20,
        Signal.CARDINALITY: 0.20,
        Signal.RECALL: 0.02,
    }
    return SignalEvidence(
        signal=signal,
        complete=False,
        reference_count=reference_count,
        current_count=current_count,
        statistic=None,
        effect=None,
        effect_floor=floors[signal],
        raw_p_value=None,
        reason=reason,
    )


def _run_signal(
    signal: Signal,
    *,
    reference_count: int,
    current_count: int,
    operation: Callable[[], SignalEvidence],
    reasons: list[str],
) -> SignalEvidence:
    try:
        return operation()
    except Exception as exc:
        _append(reasons, f"SIGNAL_TEST_EXCEPTION:{signal.value}")
        return _signal_failure(
            signal,
            reference_count=reference_count,
            current_count=current_count,
            reason=f"{type(exc).__name__}:{exc}",
        )


def _finalize(
    *,
    metric: Metric,
    current_window: AssembledShadowWindow,
    signals: Sequence[SignalEvidence],
    reasons: Sequence[str],
) -> WindowEvidence:
    try:
        return finalize_window_evidence(
            metric=metric,
            window_id=current_window.window_id,
            signals=signals,
            eligible_query_count=ELIGIBLE_QUERY_COUNT,
            audit_query_count=AUDIT_QUERY_COUNT,
            prerequisites_valid=not reasons,
            reason_codes=reasons,
        )
    except (TypeError, ValueError) as exc:
        return _incomplete(
            current=current_window,
            metric=metric,
            reasons=(*reasons, f"FINALIZE_EXCEPTION:{type(exc).__name__}"),
        )


def extract_window_evidence(
    *,
    reference_window: AssembledShadowWindow,
    current_window: AssembledShadowWindow,
    metric: Metric | str,
    detector_seed: int,
) -> WindowEvidence:
    """Extract one current-window statistical family from two raw windows.

    The reference and current audit samples are selected independently, using
    their own immutable window IDs.  Every invalid prerequisite yields a
    fail-closed incomplete value; no data is imputed or re-queried.
    """

    reasons: list[str] = []
    try:
        normalized_metric = Metric(metric)
    except (TypeError, ValueError):
        return _incomplete(
            current=current_window,
            metric=metric,
            reasons=("METRIC_PARAMETER_INVALID",),
        )
    if not isinstance(reference_window, AssembledShadowWindow):
        _append(reasons, "REFERENCE_WINDOW_INVALID")
    elif not reference_window.complete:
        _append(reasons, "REFERENCE_WINDOW_INCOMPLETE")
    if not isinstance(current_window, AssembledShadowWindow):
        _append(reasons, "CURRENT_WINDOW_INVALID")
    elif not current_window.complete:
        _append(reasons, "CURRENT_WINDOW_INCOMPLETE")
    if reasons:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)
    assert isinstance(reference_window, AssembledShadowWindow)
    assert isinstance(current_window, AssembledShadowWindow)
    if reference_window.window_id == current_window.window_id:
        _append(reasons, "WINDOW_IDS_MUST_DIFFER")
    if reference_window.metric != current_window.metric:
        _append(reasons, "WINDOW_METRIC_MISMATCH")
    if (
        reference_window.metric != normalized_metric
        or current_window.metric != normalized_metric
    ):
        _append(reasons, "METRIC_PARAMETER_MISMATCH")
    if reference_window.threshold_stratum != current_window.threshold_stratum:
        _append(reasons, "THRESHOLD_STRATUM_MISMATCH")
    if reasons:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)

    reference_identity = _identity_bundle(
        reference_window, label="REFERENCE", metric=normalized_metric, reasons=reasons
    )
    current_identity = _identity_bundle(
        current_window, label="CURRENT", metric=normalized_metric, reasons=reasons
    )
    if reference_identity is None or current_identity is None:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)
    _compare_identities(reference_identity, current_identity, reasons=reasons)
    reference_records = _canonical_record_map(reference_window, label="REFERENCE", reasons=reasons)
    current_records = _canonical_record_map(current_window, label="CURRENT", reasons=reasons)
    if reasons or reference_records is None or current_records is None:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)

    reference_selection = _selection(
        reference_window,
        detector_seed=detector_seed,
        metric=normalized_metric,
        label="REFERENCE",
        reasons=reasons,
    )
    current_selection = _selection(
        current_window,
        detector_seed=detector_seed,
        metric=normalized_metric,
        label="CURRENT",
        reasons=reasons,
    )
    if reference_selection is None or current_selection is None:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)
    reference_audit = _selected_records(
        reference_selection, reference_records, label="REFERENCE", reasons=reasons
    )
    current_audit = _selected_records(
        current_selection, current_records, label="CURRENT", reasons=reasons
    )
    if reference_audit is None or current_audit is None:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)
    reference_recall = _recall_sample(
        window=reference_window,
        identity=reference_identity,
        selection=reference_selection,
        records=reference_audit,
        label="REFERENCE",
        reasons=reasons,
    )
    current_recall = _recall_sample(
        window=current_window,
        identity=current_identity,
        selection=current_selection,
        records=current_audit,
        label="CURRENT",
        reasons=reasons,
    )
    if reference_recall is None or current_recall is None:
        return _incomplete(current=current_window, metric=normalized_metric, reasons=reasons)

    vector = _run_signal(
        Signal.QUERY_VECTOR,
        reference_count=ELIGIBLE_QUERY_COUNT,
        current_count=ELIGIBLE_QUERY_COUNT,
        operation=lambda: query_vector_signal_test(
            tuple(record.query_vector for record in reference_window.query_records),
            tuple(record.query_vector for record in current_window.query_records),
            metric=normalized_metric,
            detector_seed=detector_seed,
            window_id=current_window.window_id,
        ),
        reasons=reasons,
    )
    threshold = _run_signal(
        Signal.THRESHOLD,
        reference_count=ELIGIBLE_QUERY_COUNT,
        current_count=ELIGIBLE_QUERY_COUNT,
        operation=lambda: ks_signal_test(
            tuple(record.threshold_radius for record in reference_window.query_records),
            tuple(record.threshold_radius for record in current_window.query_records),
            signal=Signal.THRESHOLD,
            metric=normalized_metric,
            detector_seed=detector_seed,
            window_id=current_window.window_id,
        ),
        reasons=reasons,
    )
    cardinality = _run_signal(
        Signal.CARDINALITY,
        reference_count=AUDIT_QUERY_COUNT,
        current_count=AUDIT_QUERY_COUNT,
        operation=lambda: ks_signal_test(
            tuple(record.exact_cardinality for record in reference_audit),
            tuple(record.exact_cardinality for record in current_audit),
            signal=Signal.CARDINALITY,
            metric=normalized_metric,
            detector_seed=detector_seed,
            window_id=current_window.window_id,
        ),
        reasons=reasons,
    )
    recall = _run_signal(
        Signal.RECALL,
        reference_count=AUDIT_QUERY_COUNT,
        current_count=AUDIT_QUERY_COUNT,
        operation=lambda: recall_signal_test(
            reference_recall, current_recall, detector_seed=detector_seed
        ),
        reasons=reasons,
    )
    signals = (vector, threshold, cardinality, recall)
    return _finalize(
        metric=normalized_metric,
        current_window=current_window,
        signals=signals,
        reasons=reasons,
    )


__all__ = ["extract_window_evidence"]
