// SIMULATED DATA — this prototype is not connected to any Milvus deployment
// or VD backend. Every value below is fabricated for visual design purposes.

export type StageState = "verified" | "blocked" | "inactive";

export interface Stage {
  id: string;
  label: string;
  state: StageState;
  summary: string;
  detail: string;
  reasonCode?: string;
  dependency?: string;
  evidence?: { label: string; value: string; mono?: boolean }[];
}

export const stages: Stage[] = [
  {
    id: "observed",
    label: "Observed",
    state: "verified",
    summary: "Workload observation window closed",
    detail:
      "1,200 query observations collected from workload identity wl-search-api-r4 over a 15 minute window.",
    dependency: "Telemetry ingestion",
    evidence: [
      { label: "workload identity", value: "wl-search-api-r4", mono: true },
      { label: "observations", value: "1,200", mono: true },
      { label: "window closed", value: "2026-08-09 13:45:00Z", mono: true },
    ],
  },
  {
    id: "predicted",
    label: "Predicted",
    state: "verified",
    summary: "Response profile computed for four ef values",
    detail:
      "Predicted capped recall and p95 latency were computed with bootstrap intervals. Prediction is evidence only. It does not authorize execution.",
    dependency: "Stage-1 observation ✓",
    evidence: [
      { label: "profile digest", value: "a83e5c17…19", mono: true },
      { label: "estimator", value: "bootstrap-2k", mono: true },
      { label: "computed at", value: "2026-08-09 13:46:12Z", mono: true },
    ],
  },
  {
    id: "qualified",
    label: "Qualified",
    state: "verified",
    summary: "Last-known-good ef 400 re-qualified",
    detail:
      "LKG ef 400 satisfies the recall floor and latency ceiling of control profile prod-conservative. Qualification does not authorize routing.",
    dependency: "Stage-2 prediction ✓",
    evidence: [
      { label: "lkg ef", value: "400", mono: true },
      { label: "control profile", value: "prod-conservative", mono: true },
      { label: "qualified at", value: "2026-08-09 13:41:58Z", mono: true },
    ],
  },
  {
    id: "admitted",
    label: "Admitted",
    state: "verified",
    summary: "Candidate ef 800 admitted for evaluation",
    detail:
      "The candidate transition passed admission checks and holds a receipt. Admission is not a signed grant and confers no authority to route traffic.",
    dependency: "Stage-3 qualification ✓",
    evidence: [
      { label: "admission receipt", value: "adm-eval 6c22…f1", mono: true },
      { label: "candidate ef", value: "800", mono: true },
      { label: "admitted at", value: "2026-08-09 13:42:07Z", mono: true },
    ],
  },
  {
    id: "authorized",
    label: "Authorized",
    state: "blocked",
    summary: "Signed activation grant is unavailable",
    detail:
      "No signed activation grant was presented by the authority service for this candidate. VD fails closed: downstream routing and execution remain unavailable.",
    reasonCode: "SIGNED_GRANT_REQUIRED",
    dependency: "Stage-4 admission ✓",
    evidence: [
      { label: "admission receipt", value: "adm-eval 6c22…f1", mono: true },
      { label: "profile digest", value: "a83e5c17…19", mono: true },
      { label: "workload identity", value: "wl-search-api-r4", mono: true },
      { label: "evaluated at", value: "2026-08-09 14:00:03Z", mono: true },
    ],
  },
  {
    id: "routed",
    label: "Routed",
    state: "inactive",
    summary: "Not reached",
    detail:
      "Routing is unavailable because authorization did not complete. Traffic continues to serve the last-known-good configuration.",
    dependency: "Stage-5 authorization ×",
  },
  {
    id: "executed",
    label: "Executed",
    state: "inactive",
    summary: "Not reached",
    detail:
      "No execution evidence exists for the candidate. Signed authorization would not by itself prove execution; execution evidence is recorded separately.",
    dependency: "Stage-6 routing —",
  },
];

export interface EfPoint {
  ef: number;
  recall: number;
  recallLcb: number;
  recallUcb: number;
  p95: number;
  p95Lcb: number;
  p95Ucb: number;
  role?: "serving" | "candidate";
}

export const responseProfile: EfPoint[] = [
  {
    ef: 200,
    recall: 0.872,
    recallLcb: 0.858,
    recallUcb: 0.884,
    p95: 11.4,
    p95Lcb: 10.7,
    p95Ucb: 12.3,
  },
  {
    ef: 400,
    recall: 0.931,
    recallLcb: 0.921,
    recallUcb: 0.939,
    p95: 18.2,
    p95Lcb: 17.3,
    p95Ucb: 19.4,
    role: "serving",
  },
  {
    ef: 800,
    recall: 0.964,
    recallLcb: 0.951,
    recallUcb: 0.973,
    p95: 31.7,
    p95Lcb: 29.8,
    p95Ucb: 34.6,
    role: "candidate",
  },
  {
    ef: 1600,
    recall: 0.978,
    recallLcb: 0.958,
    recallUcb: 0.989,
    p95: 58.9,
    p95Lcb: 54.1,
    p95Ucb: 66.2,
  },
];

export interface ActivityEvent {
  time: string;
  title: string;
  detail: string;
  tone?: "neutral" | "blocked";
}

export const activity: ActivityEvent[] = [
  {
    time: "14:00",
    title: "Authorization evaluated",
    detail: "Signed activation grant absent — SIGNED_GRANT_REQUIRED",
    tone: "blocked",
  },
  { time: "13:42", title: "Candidate evaluation completed", detail: "ef 800" },
  { time: "13:41", title: "LKG qualification verified", detail: "ef 400" },
  {
    time: "13:39",
    title: "Activation refused",
    detail: "Signed grant absent",
    tone: "blocked",
  },
  {
    time: "13:31",
    title: "Response profile refreshed",
    detail: "1,200 observations",
  },
  {
    time: "13:18",
    title: "Drift evaluation completed",
    detail: "Regime unchanged — L2 · target-075",
  },
  {
    time: "12:57",
    title: "Rollback path verified",
    detail: "Restoration target ef 400",
  },
];

export const navItems = [
  { label: "Overview", to: "/" },
  { label: "Drift Intelligence", to: "/drift" },
  { label: "Response Intelligence", to: "/response" },
  { label: "LKG Qualification", to: "/lkg" },
  { label: "Canary Operations", to: "/canary" },
  { label: "Safety & Authority", to: "/safety" },
  { label: "Audit & Evidence", to: "/audit" },
  { label: "Control Profile", to: "/control-profile" },
  { label: "Health", to: "/health" },
] as const;

/* ---------------------------------------------------------------- drift */

export interface DriftWindow {
  id: string;
  time: string;
  mmd2: number;
  threshold: number;
  p: number;
  holm: number;
  decision: "no breach" | "breach" | "inconclusive";
}

export const driftWindows: DriftWindow[] = [
  {
    id: "w-118",
    time: "13:18",
    mmd2: 0.0121,
    threshold: 0.021,
    p: 0.412,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-117",
    time: "13:03",
    mmd2: 0.0138,
    threshold: 0.021,
    p: 0.301,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-116",
    time: "12:48",
    mmd2: 0.0184,
    threshold: 0.021,
    p: 0.104,
    holm: 0.624,
    decision: "inconclusive",
  },
  {
    id: "w-115",
    time: "12:33",
    mmd2: 0.0092,
    threshold: 0.021,
    p: 0.588,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-114",
    time: "12:18",
    mmd2: 0.0231,
    threshold: 0.021,
    p: 0.021,
    holm: 0.147,
    decision: "inconclusive",
  },
  {
    id: "w-113",
    time: "12:03",
    mmd2: 0.0104,
    threshold: 0.021,
    p: 0.499,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-112",
    time: "11:48",
    mmd2: 0.0087,
    threshold: 0.021,
    p: 0.64,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-111",
    time: "11:33",
    mmd2: 0.0159,
    threshold: 0.021,
    p: 0.192,
    holm: 0.96,
    decision: "no breach",
  },
  {
    id: "w-110",
    time: "11:18",
    mmd2: 0.0143,
    threshold: 0.021,
    p: 0.268,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-109",
    time: "11:03",
    mmd2: 0.0116,
    threshold: 0.021,
    p: 0.437,
    holm: 1.0,
    decision: "no breach",
  },
  {
    id: "w-108",
    time: "10:48",
    mmd2: 0.0201,
    threshold: 0.021,
    p: 0.058,
    holm: 0.406,
    decision: "inconclusive",
  },
  {
    id: "w-107",
    time: "10:33",
    mmd2: 0.0098,
    threshold: 0.021,
    p: 0.551,
    holm: 1.0,
    decision: "no breach",
  },
];

/* --------------------------------------------------- lkg qualification */

export interface QualWindow {
  id: string;
  epoch: 1 | 2;
  index: number;
  recall: number;
  p95: number;
  observations: number;
  verdict: "pass" | "pass (marginal)" | "excluded";
}

export const qualWindows: QualWindow[] = Array.from({ length: 12 }, (_, i) => {
  const epoch = (i < 6 ? 1 : 2) as 1 | 2;
  const recallSeq = [
    0.934, 0.929, 0.938, 0.926, 0.933, 0.931, 0.936, 0.928, 0.941, 0.924, 0.932, 0.935,
  ];
  const p95Seq = [17.9, 18.6, 17.4, 19.1, 18.0, 18.3, 17.7, 18.9, 17.2, 19.4, 18.1, 17.8];
  const verdict: QualWindow["verdict"] =
    i === 9 ? "pass (marginal)" : i === 3 ? "pass (marginal)" : "pass";
  return {
    id: `q-${100 + i}`,
    epoch,
    index: (i % 6) + 1,
    recall: recallSeq[i]!,
    p95: p95Seq[i]!,
    observations: 96 + ((i * 13) % 41),
    verdict,
  };
});

/* ------------------------------------------------------------ evidence */

export interface EvidenceEntry {
  seq: number;
  time: string;
  kind: string;
  subject: string;
  digest: string;
  verification: "verified" | "unverified" | "refused";
  tone?: "blocked";
}

export const evidenceLedger: EvidenceEntry[] = [
  {
    seq: 4417,
    time: "2026-08-09 14:00:03Z",
    kind: "authorization.evaluated",
    subject: "candidate ef 800",
    digest: "9d41ab07…c2",
    verification: "refused",
    tone: "blocked",
  },
  {
    seq: 4416,
    time: "2026-08-09 13:42:07Z",
    kind: "admission.receipt",
    subject: "adm-eval 6c22…f1",
    digest: "6c22e930…f1",
    verification: "verified",
  },
  {
    seq: 4415,
    time: "2026-08-09 13:41:58Z",
    kind: "qualification.completed",
    subject: "lkg ef 400",
    digest: "b7714c2e…08",
    verification: "verified",
  },
  {
    seq: 4414,
    time: "2026-08-09 13:39:11Z",
    kind: "activation.refused",
    subject: "SIGNED_GRANT_REQUIRED",
    digest: "1f5a8d44…7b",
    verification: "refused",
    tone: "blocked",
  },
  {
    seq: 4413,
    time: "2026-08-09 13:31:44Z",
    kind: "profile.refreshed",
    subject: "1,200 observations",
    digest: "a83e5c17…19",
    verification: "verified",
  },
  {
    seq: 4412,
    time: "2026-08-09 13:18:02Z",
    kind: "drift.evaluated",
    subject: "regime L2 · target-075",
    digest: "40cc91ea…d5",
    verification: "verified",
  },
  {
    seq: 4411,
    time: "2026-08-09 12:57:36Z",
    kind: "rollback.path.verified",
    subject: "restoration target ef 400",
    digest: "2ee6b013…9a",
    verification: "verified",
  },
  {
    seq: 4410,
    time: "2026-08-09 12:44:19Z",
    kind: "observation.window.closed",
    subject: "wl-search-api-r4",
    digest: "77b0d5c8…31",
    verification: "verified",
  },
];

/* -------------------------------------------------------------- health */

export interface HealthItem {
  component: string;
  statement: string;
  source: string;
  checked: string;
  state: "healthy" | "stale" | "degraded" | "unknown";
}

export const healthItems: HealthItem[] = [
  {
    component: "Milvus cluster",
    statement: "Query nodes responding · 4/4",
    source: "milvus /healthz probe",
    checked: "14:02:11Z",
    state: "healthy",
  },
  {
    component: "Index (HNSW · M 32)",
    statement: "Loaded, no compaction in flight",
    source: "milvus index status",
    checked: "14:02:11Z",
    state: "healthy",
  },
  {
    component: "Telemetry ingestion",
    statement: "1,200 observations / 15 min",
    source: "observation collector",
    checked: "14:01:50Z",
    state: "healthy",
  },
  {
    component: "Response profile worker",
    statement: "Last successful run 14 min ago",
    source: "worker heartbeat",
    checked: "13:48:02Z",
    state: "stale",
  },
  {
    component: "Evidence ledger",
    statement: "Append-only chain intact to seq 4417",
    source: "ledger self-verification",
    checked: "14:02:04Z",
    state: "healthy",
  },
  {
    component: "Authority service",
    statement: "No signed activation grant available",
    source: "authority service response",
    checked: "14:00:03Z",
    state: "degraded",
  },
  {
    component: "Canary router",
    statement: "No candidate partition provisioned",
    source: "router control-plane read",
    checked: "14:01:22Z",
    state: "unknown",
  },
  {
    component: "Rollback executor",
    statement: "Restoration path verified to ef 400",
    source: "rollback preflight",
    checked: "12:57:36Z",
    state: "healthy",
  },
];

/* ----------------------------------------------------- control profile */

export interface ControlValue {
  key: string;
  value: string;
  kind: "operator" | "invariant";
  note: string;
}

export const controlProfile: ControlValue[] = [
  {
    key: "profile.name",
    value: "prod-conservative",
    kind: "operator",
    note: "Selected control profile",
  },
  { key: "profile.revision", value: "r14", kind: "operator", note: "Immutable once published" },
  {
    key: "recall.floor",
    value: "0.920",
    kind: "operator",
    note: "Capped recall floor for qualification",
  },
  {
    key: "latency.p95.ceiling",
    value: "24.0 ms",
    kind: "operator",
    note: "Ceiling applied to predicted p95",
  },
  {
    key: "ef.search.space",
    value: "200 · 400 · 800 · 1600",
    kind: "operator",
    note: "Permitted ef ladder",
  },
  {
    key: "qualification.epochs",
    value: "2",
    kind: "invariant",
    note: "Protocol invariant — not operator-configurable",
  },
  {
    key: "qualification.windows",
    value: "12",
    kind: "invariant",
    note: "Protocol invariant — not operator-configurable",
  },
  {
    key: "authorization.mode",
    value: "signed-grant-required",
    kind: "invariant",
    note: "Fail-closed; cannot be relaxed",
  },
  {
    key: "rollback.supremacy",
    value: "enabled",
    kind: "invariant",
    note: "Rollback outranks all activation paths",
  },
  {
    key: "frontend.authority",
    value: "none",
    kind: "invariant",
    note: "This interface never establishes authority",
  },
];
