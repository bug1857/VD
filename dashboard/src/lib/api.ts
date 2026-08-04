/**
 * Centralized API surface for the VD Control Center.
 *
 * PHASE A (this build) is frontend-only:
 *  - GET methods resolve demo data asynchronously. No fetch is performed.
 *  - The stream adapter is a disconnected no-op.
 *  - Command methods perform no fetch/POST and throw a typed
 *    BackendNotConnectedError.
 *
 * PHASE B will replace the bodies below with real fetch calls to the Python
 * control service. Endpoint strings, method names, and envelope shapes are
 * intentionally frozen here so no component ever builds a URL itself.
 */

import {
  getDemoAudit,
  getDemoCanary,
  getDemoCommands,
  getDemoDrift,
  getDemoEvents,
  getDemoSafetyGates,
  getDemoStatus,
  getDemoTelemetry,
} from "./demo-data";
import {
  BackendNotConnectedError,
  type ApiEnvelope,
  type AuditPayload,
  type CanaryPayload,
  type CommandRequest,
  type CommandsPayload,
  type DemoScenario,
  type DriftPayload,
  type DriftRange,
  type EventsPayload,
  type SafetyGatesPayload,
  type StatusPayload,
  type StreamAdapter,
  type StreamEvent,
  type TelemetryPayload,
} from "./types";

export const API_BASE = "/api/v1";

export const API_ENDPOINTS = {
  status: `${API_BASE}/status`,
  drift: `${API_BASE}/drift`,
  telemetry: `${API_BASE}/telemetry`,
  canary: `${API_BASE}/canary`,
  events: `${API_BASE}/events`,
  safetyGates: `${API_BASE}/safety-gates`,
  audit: `${API_BASE}/audit`,
  commands: `${API_BASE}/commands`,
  stream: `${API_BASE}/stream`,
  verifyGrant: `${API_BASE}/commands/verify-grant`,
  startCanary: `${API_BASE}/commands/start-canary`,
  pauseRouting: `${API_BASE}/commands/pause-routing`,
  requestRollback: `${API_BASE}/commands/request-rollback`,
  exportAudit: `${API_BASE}/commands/export-audit`,
} as const;

export type ApiEndpointKey = keyof typeof API_ENDPOINTS;

export const ENDPOINT_CATALOG: {
  key: ApiEndpointKey;
  method: "GET" | "POST";
  path: string;
  purpose: string;
  phaseA: string;
}[] = [
  {
    key: "status",
    method: "GET",
    path: API_ENDPOINTS.status,
    purpose: "Mode, health, identities, KPI strip",
    phaseA: "Demo data",
  },
  {
    key: "drift",
    method: "GET",
    path: API_ENDPOINTS.drift,
    purpose: "MMD², KS/Holm, breach, composition",
    phaseA: "Demo data",
  },
  {
    key: "telemetry",
    method: "GET",
    path: API_ENDPOINTS.telemetry,
    purpose: "Recall, latency, throughput, SLO",
    phaseA: "Demo data",
  },
  {
    key: "canary",
    method: "GET",
    path: API_ENDPOINTS.canary,
    purpose: "LKG/candidate split, lifecycle, schedule",
    phaseA: "Demo data",
  },
  {
    key: "events",
    method: "GET",
    path: API_ENDPOINTS.events,
    purpose: "Event timeline backfill",
    phaseA: "Demo data",
  },
  {
    key: "safetyGates",
    method: "GET",
    path: API_ENDPOINTS.safetyGates,
    purpose: "Safety gate evaluation",
    phaseA: "Demo data",
  },
  {
    key: "audit",
    method: "GET",
    path: API_ENDPOINTS.audit,
    purpose: "Audit records, hashes, experiments",
    phaseA: "Demo data",
  },
  {
    key: "commands",
    method: "GET",
    path: API_ENDPOINTS.commands,
    purpose: "Command history",
    phaseA: "Demo data (empty)",
  },
  {
    key: "stream",
    method: "GET",
    path: API_ENDPOINTS.stream,
    purpose: "SSE telemetry/event stream",
    phaseA: "Disconnected no-op adapter",
  },
  {
    key: "verifyGrant",
    method: "POST",
    path: API_ENDPOINTS.verifyGrant,
    purpose: "Verify operator grant",
    phaseA: "Throws BACKEND_NOT_CONNECTED",
  },
  {
    key: "startCanary",
    method: "POST",
    path: API_ENDPOINTS.startCanary,
    purpose: "Start candidate canary",
    phaseA: "Throws BACKEND_NOT_CONNECTED",
  },
  {
    key: "pauseRouting",
    method: "POST",
    path: API_ENDPOINTS.pauseRouting,
    purpose: "Pause adaptive routing",
    phaseA: "Throws BACKEND_NOT_CONNECTED",
  },
  {
    key: "requestRollback",
    method: "POST",
    path: API_ENDPOINTS.requestRollback,
    purpose: "Request rollback to LKG",
    phaseA: "Throws BACKEND_NOT_CONNECTED",
  },
  {
    key: "exportAudit",
    method: "POST",
    path: API_ENDPOINTS.exportAudit,
    purpose: "Export signed audit bundle",
    phaseA: "Throws BACKEND_NOT_CONNECTED",
  },
];

export const BACKEND_NOT_CONNECTED_LABEL = "Backend not connected";

const DEMO_LATENCY_MS = 420;

/** Resolves demo data asynchronously. `loading` never settles, by design. */
function resolveDemo<T>(
  scenario: DemoScenario,
  produce: () => ApiEnvelope<T>,
): Promise<ApiEnvelope<T>> {
  if (scenario === "loading") {
    return new Promise<ApiEnvelope<T>>(() => {
      /* intentionally pending: demonstrates the loading state */
    });
  }
  return new Promise((resolve) => setTimeout(() => resolve(produce()), DEMO_LATENCY_MS));
}

/* ------------------------------- GET ------------------------------- */

export const getStatus = (scenario: DemoScenario): Promise<ApiEnvelope<StatusPayload>> =>
  resolveDemo(scenario, () => getDemoStatus(scenario));

export const getDrift = (
  scenario: DemoScenario,
  range: DriftRange = "24h",
): Promise<ApiEnvelope<DriftPayload>> => resolveDemo(scenario, () => getDemoDrift(scenario, range));

export const getTelemetry = (scenario: DemoScenario): Promise<ApiEnvelope<TelemetryPayload>> =>
  resolveDemo(scenario, () => getDemoTelemetry(scenario));

export const getCanary = (scenario: DemoScenario): Promise<ApiEnvelope<CanaryPayload>> =>
  resolveDemo(scenario, () => getDemoCanary(scenario));

export const getEvents = (scenario: DemoScenario): Promise<ApiEnvelope<EventsPayload>> =>
  resolveDemo(scenario, () => getDemoEvents(scenario));

export const getSafetyGates = (scenario: DemoScenario): Promise<ApiEnvelope<SafetyGatesPayload>> =>
  resolveDemo(scenario, () => getDemoSafetyGates(scenario));

export const getAudit = (scenario: DemoScenario): Promise<ApiEnvelope<AuditPayload>> =>
  resolveDemo(scenario, () => getDemoAudit(scenario));

export const getCommands = (scenario: DemoScenario): Promise<ApiEnvelope<CommandsPayload>> =>
  resolveDemo(scenario, () => getDemoCommands(scenario));

/* ------------------------------ STREAM ----------------------------- */

/**
 * Phase A stream adapter: permanently disconnected. It opens no EventSource,
 * emits no events, and never fabricates a connection. Phase B will implement
 * Last-Event-ID reconnect against API_ENDPOINTS.stream.
 */
export function createStreamAdapter(): StreamAdapter {
  const listeners = new Set<(event: StreamEvent) => void>();
  return {
    state: "DISCONNECTED",
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() {
      listeners.clear();
    },
  };
}

/* ------------------------------ COMMANDS --------------------------- */

function rejectCommand(endpoint: string): never {
  throw new BackendNotConnectedError(
    BACKEND_NOT_CONNECTED_LABEL,
    `${endpoint} is not reachable in Phase A. No request was sent and no state changed.`,
  );
}

export const postVerifyGrant = async (_request: CommandRequest): Promise<never> =>
  rejectCommand(API_ENDPOINTS.verifyGrant);

export const postStartCanary = async (_request: CommandRequest): Promise<never> =>
  rejectCommand(API_ENDPOINTS.startCanary);

export const postPauseRouting = async (_request: CommandRequest): Promise<never> =>
  rejectCommand(API_ENDPOINTS.pauseRouting);

export const postRequestRollback = async (_request: CommandRequest): Promise<never> =>
  rejectCommand(API_ENDPOINTS.requestRollback);

export const postExportAudit = async (_request: CommandRequest): Promise<never> =>
  rejectCommand(API_ENDPOINTS.exportAudit);

export const COMMAND_ENDPOINT_BY_KIND = {
  VERIFY_GRANT: API_ENDPOINTS.verifyGrant,
  START_CANARY: API_ENDPOINTS.startCanary,
  PAUSE_ROUTING: API_ENDPOINTS.pauseRouting,
  REQUEST_ROLLBACK: API_ENDPOINTS.requestRollback,
  EXPORT_AUDIT: API_ENDPOINTS.exportAudit,
} as const;
