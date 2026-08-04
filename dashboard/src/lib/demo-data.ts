/**
 * DEMO DATA — every value in this file is fabricated for UI development.
 *
 * This is the single source of truth for all numbers rendered by the VD
 * Control Center in Phase A. There is no backend, no database, no Milvus
 * access, and nothing here is persisted. Components must read values from
 * these payloads and never hard-code ef, candidate counts, traffic splits,
 * or last-known-good identity.
 */

import type {
  ApiEnvelope,
  AuditPayload,
  CanaryPayload,
  CommandsPayload,
  Connectivity,
  DemoScenario,
  DriftPayload,
  DriftRange,
  EventsPayload,
  Freshness,
  SafetyGatesPayload,
  StatusPayload,
  TelemetryPayload,
} from "./types";

export const DEMO_DATA_LABEL = "Demo data";
export const DEMO_DATA_NOTICE =
  "Demo data — generated in the browser for interface development. No backend, database, or vector store is connected.";

const BASE_TIME = Date.parse("2026-08-04T08:40:00.000Z");

const iso = (offsetMinutes: number) => new Date(BASE_TIME + offsetMinutes * 60_000).toISOString();

const series = (count: number, stepMinutes: number, fn: (i: number) => number) =>
  Array.from({ length: count }, (_, i) => ({
    t: iso(-(count - 1 - i) * stepMinutes),
    v: fn(i),
  }));

const wave = (i: number, base: number, amp: number, period: number) =>
  base + amp * Math.sin((i / period) * Math.PI * 2);

/* ------------------------------------------------------------------ */
/* Scenario envelope scaffolding                                       */
/* ------------------------------------------------------------------ */

const FRESH: Freshness = {
  state: "FRESH",
  observedAt: iso(-1),
  ageSeconds: 42,
  ttlSeconds: 120,
  source: "demo-fixture",
};

const STALE: Freshness = {
  state: "STALE",
  observedAt: iso(-97),
  ageSeconds: 5_820,
  ttlSeconds: 120,
  source: "demo-fixture",
};

const UNKNOWN_FRESHNESS: Freshness = { state: "UNKNOWN", source: "demo-fixture" };

const CONNECTED: Connectivity = {
  state: "CONNECTED",
  endpoint: "http://localhost:8000/api/v1",
  lastContactAt: iso(-1),
  detail: "Demo adapter — no network call performed",
};

const DISCONNECTED: Connectivity = {
  state: "DISCONNECTED",
  endpoint: "http://localhost:8000/api/v1",
  lastContactAt: iso(-64),
  detail: "Backend not connected",
};

const UNAUTHORIZED: Connectivity = {
  state: "UNAUTHORIZED",
  endpoint: "http://localhost:8000/api/v1",
  detail: "No verified operator grant present",
};

let requestCounter = 0;

function envelope<T>(scenario: DemoScenario, data: T, emptyData: T): ApiEnvelope<T> {
  requestCounter += 1;
  const requestId = `demo-${String(requestCounter).padStart(5, "0")}`;
  const generatedAt = iso(0);
  const base = { requestId, generatedAt, demo: true };

  switch (scenario) {
    case "stale":
      return {
        ...base,
        ok: true,
        data,
        freshness: STALE,
        connectivity: {
          ...CONNECTED,
          state: "DEGRADED",
          detail: "Last observation older than TTL",
        },
        notes: ["Values are outside their freshness TTL and must not be treated as current."],
      };
    case "disconnected":
      return {
        ...base,
        ok: false,
        freshness: UNKNOWN_FRESHNESS,
        connectivity: DISCONNECTED,
        error: {
          code: "BACKEND_NOT_CONNECTED",
          message: "Backend not connected",
          hint: "Phase A ships the interface only. The Python control service is not wired up.",
          retryable: false,
        },
      };
    case "unauthorized":
      return {
        ...base,
        ok: false,
        freshness: UNKNOWN_FRESHNESS,
        connectivity: UNAUTHORIZED,
        error: {
          code: "UNAUTHORIZED",
          message: "Operator grant not verified",
          hint: "A verified grant is required before any control surface returns data.",
          retryable: false,
        },
      };
    case "blocked":
      return {
        ...base,
        ok: false,
        freshness: FRESH,
        connectivity: CONNECTED,
        error: {
          code: "BLOCKED_BY_SAFETY_GATE",
          message: "Blocked by safety gate",
          hint: "One or more required gates is failing; read-through is suppressed by policy.",
          retryable: false,
        },
      };
    case "error":
      return {
        ...base,
        ok: false,
        freshness: UNKNOWN_FRESHNESS,
        connectivity: { ...CONNECTED, state: "DEGRADED", detail: "Upstream returned 5xx" },
        error: {
          code: "UPSTREAM_UNAVAILABLE",
          message: "Upstream read failed",
          hint: "Retry, or inspect the control service logs once Phase B is connected.",
          retryable: true,
          occurredAt: iso(0),
        },
      };
    case "empty":
      return {
        ...base,
        ok: true,
        data: emptyData,
        freshness: { ...FRESH, ageSeconds: 6 },
        connectivity: CONNECTED,
        notes: ["No observations inside the selected window."],
      };
    case "loading":
    case "normal":
    default:
      return { ...base, ok: true, data, freshness: FRESH, connectivity: CONNECTED };
  }
}

/* ------------------------------------------------------------------ */
/* Identities                                                          */
/* ------------------------------------------------------------------ */

export const demoConfigLkg = {
  id: "demo-lkg-400",
  label: "demo HNSW last-known-good",
  revision: "demo-r1",
  hash: "sha256:2f41c9be07a5d1e4",
  params: { ef: 400, M: 16, metric: "L2" },
} as const;

export const demoConfigCandidate = {
  id: "demo-candidate-800",
  label: "demo proposed HNSW candidate",
  revision: "demo-r2",
  hash: "sha256:9ab7d240c6f81b53",
  params: { ef: 800, M: 16, metric: "L2" },
} as const;

export const demoDataset = {
  id: "demo-dataset-001",
  label: "demo DATASET-001 fixture",
  revision: "demo-v1",
  hash: "sha256:71c0aa39ff2b48d6",
  rowCount: 4_812_664,
} as const;

/* ------------------------------------------------------------------ */
/* Status                                                              */
/* ------------------------------------------------------------------ */

const statusNormal: StatusPayload = {
  mode: "DRY_RUN",
  health: "WATCH",
  connectivity: CONNECTED,
  config: demoConfigLkg,
  dataset: demoDataset,
  lkg: {
    config: demoConfigLkg,
    promotedAt: iso(-2_880),
    recall: 0.947,
    note: "Demo fixture: LKG-only routing is the safe default before a live grant.",
  },
  candidate: demoConfigCandidate,
  canaryPercent: 0,
  latestEvent: {
    id: "evt-4821",
    at: iso(-3),
    kind: "DRIFT_WINDOW_EVALUATED",
    severity: "WARNING",
    message: "MMD² above threshold for 2 of last 6 windows",
    source: "drift-detector",
  },
  outbox: { pending: 3, inFlight: 1, failed: 0, oldestPendingAgeSeconds: 74, state: "NOMINAL" },
  kpis: [
    {
      id: "mmd2",
      label: "MMD²",
      value: 0.0412,
      state: "WARNING",
      threshold: 0.035,
      detail: "6-window rolling",
    },
    {
      id: "holm",
      label: "Holm rejections",
      value: "3 / 18",
      state: "WATCH",
      detail: "α = 0.05, Holm-adjusted",
    },
    {
      id: "recall-lcb",
      label: "Recall LCB",
      value: 0.9312,
      unit: "",
      state: "NOMINAL",
      detail: "95% lower bound",
    },
    { id: "p95", label: "p95 latency", value: 41.8, unit: "ms", state: "NOMINAL", threshold: 60 },
    {
      id: "canary",
      label: "Canary traffic",
      value: 0,
      unit: "%",
      state: "WATCH",
      detail: "Candidate routing disabled in demo",
    },
    {
      id: "outbox",
      label: "Outbox pending",
      value: 3,
      state: "NOMINAL",
      detail: "1 in flight, 0 failed",
    },
  ],
};

const statusEmpty: StatusPayload = {
  mode: "UNKNOWN",
  health: "UNKNOWN",
  connectivity: { ...CONNECTED, state: "DEGRADED", detail: "No observations in window" },
  kpis: [],
};

export const getDemoStatus = (scenario: DemoScenario) =>
  envelope(scenario, statusNormal, statusEmpty);

/* ------------------------------------------------------------------ */
/* Drift                                                               */
/* ------------------------------------------------------------------ */

const DRIFT_THRESHOLD = 0.035;

const mmdSeries = series(48, 15, (i) =>
  Number((wave(i, 0.026, 0.009, 19) + (i > 38 ? 0.011 : 0) + (i % 7) * 0.0004).toFixed(5)),
).map((p) => ({ t: p.t, mmd2: p.v, threshold: DRIFT_THRESHOLD }));

const driftNormal: DriftPayload = {
  range: "24h",
  availableRanges: ["1h", "6h", "24h", "7d"],
  mmd2Series: mmdSeries,
  threshold: DRIFT_THRESHOLD,
  holmAlpha: 0.05,
  ksRows: [
    {
      feature: "embedding_norm",
      ksStatistic: 0.191,
      pValue: 0.0004,
      holmAdjustedP: 0.0072,
      rejected: true,
      alpha: 0.05,
    },
    {
      feature: "query_length",
      ksStatistic: 0.164,
      pValue: 0.0019,
      holmAdjustedP: 0.0323,
      rejected: true,
      alpha: 0.05,
    },
    {
      feature: "topk_requested",
      ksStatistic: 0.148,
      pValue: 0.0041,
      holmAdjustedP: 0.0451,
      rejected: true,
      alpha: 0.05,
    },
    {
      feature: "tenant_mix",
      ksStatistic: 0.092,
      pValue: 0.071,
      holmAdjustedP: 0.639,
      rejected: false,
      alpha: 0.05,
    },
    {
      feature: "filter_selectivity",
      ksStatistic: 0.077,
      pValue: 0.128,
      holmAdjustedP: 0.896,
      rejected: false,
      alpha: 0.05,
    },
    {
      feature: "recency_bucket",
      ksStatistic: 0.041,
      pValue: 0.412,
      holmAdjustedP: 1,
      rejected: false,
      alpha: 0.05,
    },
  ],
  breach: {
    state: "WARNING",
    detector: "MMD² (RBF kernel, median heuristic)",
    occurredAt: iso(-45),
    margin: 0.0062,
    window: "6 × 15m rolling",
  },
  detectors: [
    {
      id: "mmd",
      label: "MMD² kernel two-sample",
      state: "WARNING",
      detail: "Above threshold, 2/6 windows",
    },
    {
      id: "ks",
      label: "KS + Holm per-feature",
      state: "WATCH",
      detail: "3 of 18 features rejected",
    },
    { id: "psi", label: "PSI bucketed", state: "NOMINAL", detail: "0.07 (< 0.10)" },
    { id: "latency-shape", label: "Latency distribution shift", state: "NOMINAL" },
  ],
  sourceComposition: [
    { label: "search-api", share: 0.54 },
    { label: "batch-recluster", share: 0.21 },
    { label: "ingest-replay", share: 0.15 },
    { label: "internal-eval", share: 0.1 },
  ],
  explainability: {
    topContributors: [
      { name: "embedding_norm", contribution: 0.38 },
      { name: "query_length", contribution: 0.27 },
      { name: "topk_requested", contribution: 0.19 },
      { name: "tenant_mix", contribution: 0.1 },
      { name: "residual", contribution: 0.06 },
    ],
    note: "Attribution is a demo decomposition of the kernel statistic, not a causal claim.",
  },
};

const driftEmpty: DriftPayload = { range: "24h", mmd2Series: [], ksRows: [] };

export const getDemoDrift = (scenario: DemoScenario, range: DriftRange = "24h") => {
  const step = range === "1h" ? 4 : range === "6h" ? 12 : range === "24h" ? 48 : 56;
  return envelope<DriftPayload>(
    scenario,
    { ...driftNormal, range, mmd2Series: driftNormal.mmd2Series.slice(-step) },
    { ...driftEmpty, range },
  );
};

/* ------------------------------------------------------------------ */
/* Telemetry                                                           */
/* ------------------------------------------------------------------ */

const telemetryNormal: TelemetryPayload = {
  recallSeries: series(36, 20, (i) => Number(wave(i, 0.945, 0.008, 13).toFixed(4))).map((p, i) => ({
    t: p.t,
    recall: p.v,
    lcb: Number((p.v - 0.013 - (i % 5) * 0.0006).toFixed(4)),
  })),
  latencySeries: series(36, 20, (i) => Number(wave(i, 26.4, 3.1, 11).toFixed(2))).map((p) => ({
    t: p.t,
    meanMs: p.v,
    p95Ms: Number((p.v * 1.58).toFixed(2)),
    ucbMs: Number((p.v * 1.74).toFixed(2)),
  })),
  throughputSeries: series(36, 20, (i) => Number(wave(i, 1420, 210, 9).toFixed(0))).map((p, i) => ({
    t: p.t,
    qps: p.v,
    errorRate: Number((0.0021 + (i > 30 ? 0.0016 : 0) + (i % 4) * 0.0002).toFixed(4)),
  })),
  slo: [
    {
      id: "recall",
      objective: "Recall@10 ≥ 0.94",
      target: "0.9400",
      observed: "0.9468",
      state: "NOMINAL",
      window: "12h",
    },
    {
      id: "recall-lcb",
      objective: "Recall LCB ≥ 0.930",
      target: "0.9300",
      observed: "0.9312",
      state: "WATCH",
      window: "12h",
    },
    {
      id: "p95",
      objective: "p95 latency ≤ 60 ms",
      target: "60.0 ms",
      observed: "41.8 ms",
      state: "NOMINAL",
      window: "1h",
    },
    {
      id: "ucb",
      objective: "Latency UCB ≤ 75 ms",
      target: "75.0 ms",
      observed: "46.1 ms",
      state: "NOMINAL",
      window: "1h",
    },
    {
      id: "errors",
      objective: "Error rate ≤ 0.5%",
      target: "0.50%",
      observed: "0.37%",
      state: "WATCH",
      window: "1h",
    },
    {
      id: "availability",
      objective: "Read availability ≥ 99.9%",
      target: "99.900%",
      observed: "99.982%",
      state: "NOMINAL",
      window: "30d",
    },
  ],
  sampleSize: 184_920,
  windows: ["1h", "6h", "12h", "24h"],
  confidenceLevel: 0.95,
};

const telemetryEmpty: TelemetryPayload = {
  recallSeries: [],
  latencySeries: [],
  throughputSeries: [],
  slo: [],
};

export const getDemoTelemetry = (scenario: DemoScenario) =>
  envelope(scenario, telemetryNormal, telemetryEmpty);

/* ------------------------------------------------------------------ */
/* Canary                                                              */
/* ------------------------------------------------------------------ */

const canaryNormal: CanaryPayload = {
  lkg: {
    config: demoConfigLkg,
    promotedAt: iso(-2_880),
    recall: 0.947,
    note: "Demo fixture: all traffic remains on LKG until the backend reports an active route.",
  },
  candidate: {
    config: demoConfigCandidate,
    candidateCount: 60,
    confidence: 0.82,
    recall: 0.9521,
    recallLcb: 0.9386,
    p95Ms: 44.6,
    reason: "Demo-only proposed 400 → 800 L2 transition; backend evidence is not connected.",
    deltas: [
      { metric: "Recall@10", baseline: 0.947, candidate: 0.9521, delta: 0.0051, state: "NOMINAL" },
      {
        metric: "Recall LCB",
        baseline: 0.9312,
        candidate: 0.9386,
        delta: 0.0074,
        state: "NOMINAL",
      },
      {
        metric: "p95 latency",
        baseline: 41.8,
        candidate: 44.6,
        delta: 2.8,
        unit: "ms",
        state: "WATCH",
      },
      {
        metric: "Mean latency",
        baseline: 26.4,
        candidate: 28.9,
        delta: 2.5,
        unit: "ms",
        state: "WATCH",
      },
      {
        metric: "Error rate",
        baseline: 0.0037,
        candidate: 0.0034,
        delta: -0.0003,
        state: "NOMINAL",
      },
    ],
    evidence: [
      {
        id: "ev-1",
        label: "Stage-4 admission evidence",
        status: "UNVERIFIABLE",
        detail: "Demo fixture; the backend has not supplied verifiable evidence.",
      },
      {
        id: "ev-2",
        label: "Recall bootstrap (2,000 resamples)",
        status: "REPRODUCIBLE BUT NOT EXECUTED",
      },
      {
        id: "ev-3",
        label: "Prior promotion comparison",
        status: "HISTORICAL ONLY",
        detail: "From r6 → r7 cycle",
      },
      {
        id: "ev-4",
        label: "Latency budget attestation",
        status: "UNVERIFIABLE",
        detail: "Requires live routing data",
      },
    ],
  },
  split: {
    source: "routing-policy (demo)",
    updatedAt: iso(-18),
    slices: [
      { identityId: demoConfigLkg.id, label: demoConfigLkg.label, role: "LKG", percent: 100 },
      {
        identityId: demoConfigCandidate.id,
        label: demoConfigCandidate.label,
        role: "CANDIDATE",
        percent: 0,
      },
    ],
  },
  lifecycle: [
    {
      id: "detect",
      label: "Drift detected",
      state: "COMPLETE",
      at: iso(-180),
      detail: "MMD² breach",
    },
    {
      id: "propose",
      label: "Candidates proposed",
      state: "COMPLETE",
      at: iso(-150),
      detail: "Search over tuning space",
    },
    { id: "offline", label: "Offline evaluation", state: "COMPLETE", at: iso(-120) },
    { id: "gate", label: "Safety gates evaluated", state: "COMPLETE", at: iso(-100) },
    {
      id: "canary",
      label: "Canary routing",
      state: "BLOCKED",
      detail: "Demo fixture: Stage 4 remains human-gated and no route is active.",
    },
    { id: "evaluate", label: "Canary evaluation window", state: "PENDING" },
    { id: "promote", label: "Promotion to LKG", state: "PENDING" },
    {
      id: "rollback",
      label: "Rollback path armed",
      state: "PENDING",
      detail: "No candidate route is active; LKG-only state is already in effect.",
    },
  ],
  schedule: {
    nextEvaluationAt: iso(28),
    cadence: "Every 30 minutes",
    window: "Rolling 6 × 15m",
    holdDownSeconds: 900,
  },
  outbox: { pending: 3, inFlight: 1, failed: 0, oldestPendingAgeSeconds: 74, state: "NOMINAL" },
};

const canaryEmpty: CanaryPayload = { lifecycle: [] };

export const getDemoCanary = (scenario: DemoScenario) =>
  envelope(scenario, canaryNormal, canaryEmpty);

/* ------------------------------------------------------------------ */
/* Events                                                              */
/* ------------------------------------------------------------------ */

const eventsNormal: EventsPayload = {
  lastEventId: "evt-4821",
  items: [
    {
      id: "evt-4821",
      at: iso(-3),
      kind: "DRIFT_WINDOW_EVALUATED",
      severity: "WARNING",
      message: "MMD² 0.0412 above threshold 0.0350",
      source: "drift-detector",
    },
    {
      id: "evt-4820",
      at: iso(-18),
      kind: "ROUTING_SPLIT_UPDATED",
      severity: "INFO",
      message: "Routing split refreshed from policy",
      source: "router",
    },
    {
      id: "evt-4819",
      at: iso(-34),
      kind: "TELEMETRY_WINDOW_CLOSED",
      severity: "SUCCESS",
      message: "Recall LCB 0.9312 within objective",
      source: "telemetry",
    },
    {
      id: "evt-4818",
      at: iso(-52),
      kind: "KS_HOLM_EVALUATED",
      severity: "WARNING",
      message: "3 of 18 features rejected at α = 0.05",
      source: "drift-detector",
    },
    {
      id: "evt-4817",
      at: iso(-92),
      kind: "CANARY_STARTED",
      severity: "INFO",
      message: "Candidate routing began",
      source: "canary-controller",
      configId: demoConfigCandidate.id,
    },
    {
      id: "evt-4816",
      at: iso(-100),
      kind: "SAFETY_GATES_EVALUATED",
      severity: "SUCCESS",
      message: "All required gates passing",
      source: "policy",
    },
    {
      id: "evt-4815",
      at: iso(-120),
      kind: "OFFLINE_EVAL_COMPLETED",
      severity: "SUCCESS",
      message: "Offline replay complete for 4 candidates",
      source: "evaluator",
    },
    {
      id: "evt-4814",
      at: iso(-150),
      kind: "CANDIDATES_PROPOSED",
      severity: "INFO",
      message: "Candidate set generated from tuning search",
      source: "tuner",
    },
    {
      id: "evt-4813",
      at: iso(-180),
      kind: "DRIFT_DETECTED",
      severity: "ERROR",
      message: "Distribution shift confirmed on embedding_norm",
      source: "drift-detector",
    },
    {
      id: "evt-4812",
      at: iso(-2_880),
      kind: "LKG_PROMOTED",
      severity: "SUCCESS",
      message: "Configuration promoted to last-known-good",
      source: "canary-controller",
      configId: demoConfigLkg.id,
    },
  ],
};

const eventsEmpty: EventsPayload = { items: [] };

export const getDemoEvents = (scenario: DemoScenario) =>
  envelope(scenario, eventsNormal, eventsEmpty);

/* ------------------------------------------------------------------ */
/* Safety gates                                                        */
/* ------------------------------------------------------------------ */

const gatesNormal: SafetyGatesPayload = {
  overall: "WARN",
  policyRevision: "policy-r12",
  gates: [
    {
      id: "grant",
      label: "Operator grant verified",
      state: "FAIL",
      required: true,
      detail: "Backend not connected — grant cannot be verified",
      evaluatedAt: iso(-1),
    },
    {
      id: "freshness",
      label: "Telemetry freshness within TTL",
      state: "PASS",
      required: true,
      evaluatedAt: iso(-1),
    },
    {
      id: "recall-floor",
      label: "Candidate recall LCB ≥ floor",
      state: "PASS",
      required: true,
      detail: "0.9386 ≥ 0.9300",
    },
    {
      id: "latency-ceiling",
      label: "Candidate latency UCB ≤ ceiling",
      state: "WARN",
      required: true,
      detail: "Within budget but trending up",
    },
    {
      id: "blast-radius",
      label: "Allocation within blast-radius cap",
      state: "PASS",
      required: true,
      detail: "Allocation sourced from routing policy",
    },
    { id: "rollback", label: "Rollback path reachable", state: "PASS", required: true },
    {
      id: "outbox",
      label: "Outbox drained below watermark",
      state: "PASS",
      required: false,
      detail: "3 pending",
    },
    {
      id: "dual-control",
      label: "Dual control for promotion",
      state: "UNKNOWN",
      required: true,
      detail: "Requires connected control service",
    },
  ],
};

const gatesEmpty: SafetyGatesPayload = { overall: "UNKNOWN", gates: [] };

export const getDemoSafetyGates = (scenario: DemoScenario) =>
  envelope(scenario, gatesNormal, gatesEmpty);

/* ------------------------------------------------------------------ */
/* Audit & evidence                                                    */
/* ------------------------------------------------------------------ */

const auditNormal: AuditPayload = {
  exportEnabled: false,
  sourceRevision: "vd-control@a91f3c7",
  experiments: [
    {
      id: "exp-r8-canary",
      label: "Adaptive retune r8 canary",
      state: "RUNNING",
      sourceRevision: "vd-control@a91f3c7",
    },
    {
      id: "exp-r7-promote",
      label: "Baseline r7 promotion",
      state: "COMPLETED",
      sourceRevision: "vd-control@6cd12ab",
    },
    {
      id: "exp-r6-shadow",
      label: "Shadow evaluation r6",
      state: "ARCHIVED",
      sourceRevision: "vd-control@41ba990",
    },
  ],
  records: [
    {
      id: "aud-9001",
      at: iso(-3),
      actor: "drift-detector",
      action: "DRIFT_WINDOW_EVALUATED",
      target: demoDataset.label,
      evidenceStatus: "FRESHLY VERIFIED",
      configHash: demoConfigLkg.hash,
      datasetHash: demoDataset.hash,
      payloadHash: "sha256:c41e0b7712ad9f30",
      experimentId: "exp-r8-canary",
      sourceRevision: "vd-control@a91f3c7",
      verification: {
        method: "in-process recompute",
        verifiedAt: iso(-2),
        verifier: "demo-verifier",
        signature: "ed25519:8f21…c0",
      },
    },
    {
      id: "aud-9000",
      at: iso(-92),
      actor: "canary-controller",
      action: "CANARY_STARTED",
      target: demoConfigCandidate.label,
      evidenceStatus: "REPRODUCIBLE BUT NOT EXECUTED",
      configHash: demoConfigCandidate.hash,
      datasetHash: demoDataset.hash,
      experimentId: "exp-r8-canary",
      sourceRevision: "vd-control@a91f3c7",
      note: "Replay bundle recorded; recompute not run in this session",
    },
    {
      id: "aud-8999",
      at: iso(-100),
      actor: "policy",
      action: "SAFETY_GATES_EVALUATED",
      evidenceStatus: "FRESHLY VERIFIED",
      configHash: demoConfigCandidate.hash,
      experimentId: "exp-r8-canary",
      sourceRevision: "vd-control@a91f3c7",
      verification: { method: "policy replay", verifiedAt: iso(-99), verifier: "demo-verifier" },
    },
    {
      id: "aud-8998",
      at: iso(-120),
      actor: "evaluator",
      action: "OFFLINE_EVAL_COMPLETED",
      target: "eval-set-frozen-04",
      evidenceStatus: "HISTORICAL ONLY",
      datasetHash: demoDataset.hash,
      payloadHash: "sha256:19aa77e3b0c4d251",
      experimentId: "exp-r8-canary",
      sourceRevision: "vd-control@a91f3c7",
    },
    {
      id: "aud-8997",
      at: iso(-181),
      actor: "tuner",
      action: "CANDIDATE_METRIC_RECONCILED",
      evidenceStatus: "CONTRADICTED",
      configHash: demoConfigCandidate.hash,
      experimentId: "exp-r8-canary",
      note: "Reported recall disagreed with recompute by 0.004 — superseded by aud-8998",
      sourceRevision: "vd-control@6cd12ab",
    },
    {
      id: "aud-8996",
      at: iso(-240),
      actor: "operator:unknown",
      action: "GRANT_VERIFICATION_ATTEMPTED",
      evidenceStatus: "UNVERIFIABLE",
      note: "No connected control service to attest the grant",
      sourceRevision: "vd-control@6cd12ab",
    },
    {
      id: "aud-8995",
      at: iso(-2_880),
      actor: "canary-controller",
      action: "LKG_PROMOTED",
      target: demoConfigLkg.label,
      evidenceStatus: "FRESHLY VERIFIED",
      configHash: demoConfigLkg.hash,
      datasetHash: demoDataset.hash,
      experimentId: "exp-r7-promote",
      sourceRevision: "vd-control@6cd12ab",
      verification: {
        method: "signed promotion record",
        verifiedAt: iso(-2_879),
        verifier: "demo-verifier",
        signature: "ed25519:2b90…7d",
      },
    },
    {
      id: "aud-8994",
      at: iso(-4_320),
      actor: "policy",
      action: "PROMOTION_BLOCKED",
      target: "cfg-cand-13ff08",
      evidenceStatus: "BLOCKED",
      configHash: "sha256:13ff08c9a7be4412",
      experimentId: "exp-r6-shadow",
      sourceRevision: "vd-control@41ba990",
      note: "Required gate failed: latency UCB above ceiling",
    },
  ],
};

const auditEmpty: AuditPayload = { records: [], experiments: [], exportEnabled: false };

export const getDemoAudit = (scenario: DemoScenario) => envelope(scenario, auditNormal, auditEmpty);

/* ------------------------------------------------------------------ */
/* Commands                                                            */
/* ------------------------------------------------------------------ */

const commandsNormal: CommandsPayload = {
  /** Phase A: submission is never enabled and history is always empty. */
  submissionEnabled: false,
  disabledReason: "Backend not connected",
  history: [],
};

export const getDemoCommands = (scenario: DemoScenario) =>
  envelope(scenario, commandsNormal, { ...commandsNormal });

/** Confirmation copy shown in command preview modals. */
export const demoCommandPreview = {
  requiredPhrase: "CONFIRM CONTROL ACTION",
  warning:
    "Control actions change live retrieval behaviour. In Phase A nothing is submitted: this preview exists to review the exact target, identities, and reason before Phase B enables submission.",
  reasonPlaceholder: "Why this action is being requested (recorded in the audit trail)",
} as const;
