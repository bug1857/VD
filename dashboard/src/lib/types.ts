/**
 * VD Control Center — shared type surface.
 *
 * Phase A is FRONTEND ONLY. These types describe the envelopes the future
 * Python REST + SSE service is expected to return. Nothing here talks to a
 * database, a backend, an auth provider, or Milvus.
 */

/* ------------------------------------------------------------------ */
/* Demo scenarios                                                      */
/* ------------------------------------------------------------------ */

export type DemoScenario =
  "normal" | "loading" | "stale" | "disconnected" | "blocked" | "unauthorized" | "empty" | "error";

export const DEMO_SCENARIOS: readonly DemoScenario[] = [
  "normal",
  "loading",
  "stale",
  "disconnected",
  "blocked",
  "unauthorized",
  "empty",
  "error",
] as const;

/* ------------------------------------------------------------------ */
/* Freshness / connectivity / status primitives                        */
/* ------------------------------------------------------------------ */

export type FreshnessState = "FRESH" | "AGING" | "STALE" | "UNKNOWN";

export interface Freshness {
  state: FreshnessState;
  observedAt?: string;
  ageSeconds?: number;
  ttlSeconds?: number;
  source?: string;
}

export type ConnectivityState = "CONNECTED" | "DEGRADED" | "DISCONNECTED" | "UNAUTHORIZED";

export interface Connectivity {
  state: ConnectivityState;
  endpoint?: string;
  lastContactAt?: string;
  detail?: string;
}

export type OperationalState = "NOMINAL" | "WATCH" | "WARNING" | "CRITICAL" | "UNKNOWN";

export type TuningMode = "DRY_RUN" | "CANARY_ENABLED" | "LKG_ONLY" | "BLOCKED" | "UNKNOWN";

/* ------------------------------------------------------------------ */
/* Errors                                                             */
/* ------------------------------------------------------------------ */

export type ApiErrorCode =
  | "BACKEND_NOT_CONNECTED"
  | "UNAUTHORIZED"
  | "BLOCKED_BY_SAFETY_GATE"
  | "STALE_DATA"
  | "UPSTREAM_UNAVAILABLE"
  | "NO_DATA"
  | "INTERNAL_ERROR";

export interface ApiError {
  code: ApiErrorCode;
  message: string;
  hint?: string;
  retryable: boolean;
  occurredAt?: string;
}

export class BackendNotConnectedError extends Error {
  readonly code: ApiErrorCode = "BACKEND_NOT_CONNECTED";
  readonly retryable = false;
  readonly detail: ApiError;

  constructor(message = "Backend not connected", hint?: string) {
    super(message);
    this.name = "BackendNotConnectedError";
    this.detail = {
      code: "BACKEND_NOT_CONNECTED",
      message,
      retryable: false,
      ...(hint === undefined ? {} : { hint }),
    };
  }
}

/* ------------------------------------------------------------------ */
/* Envelope                                                           */
/* ------------------------------------------------------------------ */

export interface ApiEnvelope<T> {
  ok: boolean;
  /** Absent whenever `ok` is false, or when the upstream window is empty. */
  data?: T;
  error?: ApiError;
  freshness: Freshness;
  connectivity: Connectivity;
  requestId: string;
  generatedAt: string;
  /** Always true in Phase A: every value is demo data. */
  demo: boolean;
  notes?: string[];
}

/* ------------------------------------------------------------------ */
/* Identities                                                         */
/* ------------------------------------------------------------------ */

export interface ConfigIdentity {
  id: string;
  label?: string;
  revision?: string;
  hash?: string;
  /** Optional tuning parameter bag — components never assume any key. */
  params?: Record<string, string | number>;
}

export interface DatasetIdentity {
  id: string;
  label?: string;
  revision?: string;
  hash?: string;
  rowCount?: number;
}

export interface LkgIdentity {
  config: ConfigIdentity;
  promotedAt?: string;
  recall?: number;
  note?: string;
}

/* ------------------------------------------------------------------ */
/* Status                                                             */
/* ------------------------------------------------------------------ */

export interface StatusPayload {
  mode: TuningMode;
  health: OperationalState;
  connectivity: Connectivity;
  config?: ConfigIdentity;
  dataset?: DatasetIdentity;
  lkg?: LkgIdentity;
  candidate?: ConfigIdentity;
  latestEvent?: EventItem;
  kpis: StatusKpi[];
  outbox?: OutboxSummary;
  canaryPercent?: number;
}

export interface StatusKpi {
  id: string;
  label: string;
  value?: number | string;
  unit?: string;
  state?: OperationalState;
  detail?: string;
  threshold?: number;
}

export interface OutboxSummary {
  pending?: number;
  inFlight?: number;
  failed?: number;
  oldestPendingAgeSeconds?: number;
  state?: OperationalState;
}

/* ------------------------------------------------------------------ */
/* Drift                                                              */
/* ------------------------------------------------------------------ */

export type DriftRange = "1h" | "6h" | "24h" | "7d";

export interface MmdPoint {
  t: string;
  mmd2: number;
  threshold?: number;
}

export interface KsRow {
  feature: string;
  ksStatistic: number;
  pValue: number;
  holmAdjustedP?: number;
  rejected: boolean;
  alpha?: number;
}

export interface DriftBreach {
  state: OperationalState;
  detector?: string;
  occurredAt?: string;
  margin?: number;
  window?: string;
}

export interface CompositionSlice {
  label: string;
  share: number;
}

export interface DriftExplainability {
  topContributors: { name: string; contribution: number }[];
  note?: string;
}

export interface DriftPayload {
  range: DriftRange;
  availableRanges?: DriftRange[];
  mmd2Series: MmdPoint[];
  threshold?: number;
  ksRows: KsRow[];
  holmAlpha?: number;
  breach?: DriftBreach;
  detectors?: { id: string; label: string; state: OperationalState; detail?: string }[];
  sourceComposition?: CompositionSlice[];
  explainability?: DriftExplainability;
}

/* ------------------------------------------------------------------ */
/* Telemetry / performance                                            */
/* ------------------------------------------------------------------ */

export interface RecallPoint {
  t: string;
  recall: number;
  lcb?: number;
}

export interface LatencyPoint {
  t: string;
  meanMs: number;
  p95Ms: number;
  ucbMs?: number;
}

export interface ThroughputPoint {
  t: string;
  qps: number;
  errorRate: number;
}

export interface SloRow {
  id: string;
  objective: string;
  target?: string;
  observed?: string;
  state: OperationalState;
  window?: string;
}

export interface TelemetryPayload {
  recallSeries: RecallPoint[];
  latencySeries: LatencyPoint[];
  throughputSeries: ThroughputPoint[];
  slo: SloRow[];
  sampleSize?: number;
  windows?: string[];
  confidenceLevel?: number;
}

/* ------------------------------------------------------------------ */
/* Canary                                                             */
/* ------------------------------------------------------------------ */

export interface TrafficSplitSlice {
  /** Identity label comes from data — never hard-coded in components. */
  identityId: string;
  label?: string;
  role: "LKG" | "CANDIDATE" | "OTHER";
  percent: number;
}

export interface TrafficSplit {
  slices: TrafficSplitSlice[];
  source?: string;
  updatedAt?: string;
}

export interface CandidateDelta {
  metric: string;
  baseline?: number;
  candidate?: number;
  delta?: number;
  unit?: string;
  state?: OperationalState;
}

export interface CandidateEvidence {
  id: string;
  label: string;
  status: EvidenceStatus;
  detail?: string;
}

export interface CandidateSummary {
  config: ConfigIdentity;
  candidateCount?: number;
  confidence?: number;
  recall?: number;
  recallLcb?: number;
  p95Ms?: number;
  reason?: string;
  deltas: CandidateDelta[];
  evidence: CandidateEvidence[];
}

export type LifecycleStageState =
  "COMPLETE" | "ACTIVE" | "PENDING" | "BLOCKED" | "FAILED" | "SKIPPED";

export interface LifecycleStage {
  id: string;
  label: string;
  state: LifecycleStageState;
  at?: string;
  detail?: string;
}

export interface CanarySchedule {
  nextEvaluationAt?: string;
  cadence?: string;
  window?: string;
  holdDownSeconds?: number;
}

export interface CanaryPayload {
  lkg?: LkgIdentity;
  candidate?: CandidateSummary;
  split?: TrafficSplit;
  lifecycle: LifecycleStage[];
  schedule?: CanarySchedule;
  outbox?: OutboxSummary;
}

/* ------------------------------------------------------------------ */
/* Events                                                            */
/* ------------------------------------------------------------------ */

export type EventSeverity = "INFO" | "SUCCESS" | "WARNING" | "ERROR";

export interface EventItem {
  id: string;
  at: string;
  kind: string;
  severity: EventSeverity;
  message: string;
  source?: string;
  configId?: string;
  datasetId?: string;
}

export interface EventsPayload {
  items: EventItem[];
  lastEventId?: string;
}

/* ------------------------------------------------------------------ */
/* Safety gates                                                       */
/* ------------------------------------------------------------------ */

export type GateState = "PASS" | "WARN" | "FAIL" | "BLOCKED" | "UNKNOWN";

export interface SafetyGate {
  id: string;
  label: string;
  state: GateState;
  required: boolean;
  detail?: string;
  evaluatedAt?: string;
}

export interface SafetyGatesPayload {
  gates: SafetyGate[];
  overall: GateState;
  policyRevision?: string;
}

/* ------------------------------------------------------------------ */
/* Audit & evidence                                                   */
/* ------------------------------------------------------------------ */

export type EvidenceStatus =
  | "FRESHLY VERIFIED"
  | "REPRODUCIBLE BUT NOT EXECUTED"
  | "HISTORICAL ONLY"
  | "CONTRADICTED"
  | "UNVERIFIABLE"
  | "BLOCKED";

export const EVIDENCE_STATUSES: readonly EvidenceStatus[] = [
  "FRESHLY VERIFIED",
  "REPRODUCIBLE BUT NOT EXECUTED",
  "HISTORICAL ONLY",
  "CONTRADICTED",
  "UNVERIFIABLE",
  "BLOCKED",
] as const;

export interface VerificationInfo {
  method?: string;
  verifiedAt?: string;
  verifier?: string;
  signature?: string;
}

export interface AuditRecord {
  id: string;
  at: string;
  actor: string;
  action: string;
  target?: string;
  evidenceStatus: EvidenceStatus;
  configHash?: string;
  datasetHash?: string;
  payloadHash?: string;
  experimentId?: string;
  sourceRevision?: string;
  verification?: VerificationInfo;
  note?: string;
}

export interface AuditPayload {
  records: AuditRecord[];
  experiments?: { id: string; label: string; state: string; sourceRevision?: string }[];
  sourceRevision?: string;
  exportEnabled: boolean;
}

/* ------------------------------------------------------------------ */
/* Commands                                                           */
/* ------------------------------------------------------------------ */

export type CommandKind =
  "VERIFY_GRANT" | "START_CANARY" | "PAUSE_ROUTING" | "REQUEST_ROLLBACK" | "EXPORT_AUDIT";

export type CommandState = "QUEUED" | "ACCEPTED" | "REJECTED" | "COMPLETED" | "FAILED";

export interface CommandRecord {
  id: string;
  kind: CommandKind;
  state: CommandState;
  submittedAt?: string;
  updatedAt?: string;
  actor?: string;
  target?: string;
  reason?: string;
  detail?: string;
}

export interface CommandsPayload {
  history: CommandRecord[];
  /** Phase A: always false. */
  submissionEnabled: boolean;
  disabledReason?: string;
}

export interface CommandRequest {
  kind: CommandKind;
  target?: string;
  configIdentity?: ConfigIdentity;
  datasetIdentity?: DatasetIdentity;
  reason?: string;
  typedPhrase?: string;
}

/* ------------------------------------------------------------------ */
/* SSE stream (discriminated union)                                   */
/* ------------------------------------------------------------------ */

export type StreamEvent =
  | { type: "status"; id: string; at: string; payload: StatusPayload }
  | { type: "drift"; id: string; at: string; payload: DriftPayload }
  | { type: "telemetry"; id: string; at: string; payload: TelemetryPayload }
  | { type: "canary"; id: string; at: string; payload: CanaryPayload }
  | { type: "event"; id: string; at: string; payload: EventItem }
  | { type: "safety"; id: string; at: string; payload: SafetyGatesPayload }
  | { type: "command"; id: string; at: string; payload: CommandRecord }
  | { type: "heartbeat"; id: string; at: string; payload: { lastEventId?: string } }
  | { type: "error"; id: string; at: string; payload: ApiError };

export interface StreamAdapter {
  readonly state: ConnectivityState;
  readonly lastEventId?: string;
  subscribe(listener: (event: StreamEvent) => void): () => void;
  close(): void;
}
