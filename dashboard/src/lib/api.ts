/**
 * Centralized API surface for the VD Control Center.
 *
 * Default mode remains frontend-only and resolves demo data asynchronously.
 *
 * Read-only integration mode is enabled with VITE_VD_API_MODE=readonly. In
 * that mode, GET and SSE paths may read from a configured VD API, while every
 * command path remains blocked locally and sends no POST request.
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

const env =
  (
    import.meta as unknown as {
      readonly env?: {
        readonly VITE_VD_API_MODE?: string;
        readonly VITE_VD_API_BASE?: string;
      };
    }
  ).env ?? {};

export type ApiMode = "demo" | "readonly";

export const API_MODE: ApiMode = env.VITE_VD_API_MODE === "readonly" ? "readonly" : "demo";
export const REMOTE_API_BASE = env.VITE_VD_API_BASE?.replace(/\/+$/, "") ?? "";
export const READONLY_COMMAND_BLOCK_LABEL = "Read-only dashboard mode";

export function isReadOnlyMode(): boolean {
  return API_MODE === "readonly";
}

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
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "drift",
    method: "GET",
    path: API_ENDPOINTS.drift,
    purpose: "MMD², KS/Holm, breach, composition",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "telemetry",
    method: "GET",
    path: API_ENDPOINTS.telemetry,
    purpose: "Recall, latency, throughput, SLO",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "canary",
    method: "GET",
    path: API_ENDPOINTS.canary,
    purpose: "LKG/candidate split, lifecycle, schedule",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "events",
    method: "GET",
    path: API_ENDPOINTS.events,
    purpose: "Event timeline backfill",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "safetyGates",
    method: "GET",
    path: API_ENDPOINTS.safetyGates,
    purpose: "Safety gate evaluation",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "audit",
    method: "GET",
    path: API_ENDPOINTS.audit,
    purpose: "Audit records, hashes, experiments",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "commands",
    method: "GET",
    path: API_ENDPOINTS.commands,
    purpose: "Command history",
    phaseA: "Demo data, or read-only fetch when enabled",
  },
  {
    key: "stream",
    method: "GET",
    path: API_ENDPOINTS.stream,
    purpose: "SSE telemetry/event stream",
    phaseA: "Disconnected no-op, or read-only SSE when enabled",
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

function endpointUrl(endpoint: string): string {
  return `${REMOTE_API_BASE}${endpoint}`;
}

function buildReadError<T>(
  endpoint: string,
  message: string,
  code: "UPSTREAM_UNAVAILABLE" | "INTERNAL_ERROR" = "UPSTREAM_UNAVAILABLE",
): ApiEnvelope<T> {
  const now = new Date().toISOString();
  return {
    ok: false,
    error: {
      code,
      message,
      hint: `${endpoint} could not be read. No command or mutation request was sent.`,
      retryable: code === "UPSTREAM_UNAVAILABLE",
      occurredAt: now,
    },
    freshness: {
      state: "UNKNOWN",
      observedAt: now,
      source: endpoint,
    },
    connectivity: {
      state: "DISCONNECTED",
      endpoint,
      detail: message,
    },
    requestId: `read-error-${crypto.randomUUID()}`,
    generatedAt: now,
    demo: false,
  };
}

function isApiEnvelope<T>(value: unknown): value is ApiEnvelope<T> {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<ApiEnvelope<T>>;
  return (
    typeof candidate.ok === "boolean" &&
    typeof candidate.freshness === "object" &&
    candidate.freshness !== null &&
    typeof candidate.connectivity === "object" &&
    candidate.connectivity !== null &&
    typeof candidate.requestId === "string" &&
    typeof candidate.generatedAt === "string" &&
    typeof candidate.demo === "boolean"
  );
}

async function readApi<T>(endpoint: string): Promise<ApiEnvelope<T>> {
  if (!isReadOnlyMode()) {
    throw new BackendNotConnectedError(
      BACKEND_NOT_CONNECTED_LABEL,
      `${endpoint} is not reachable in demo mode. No request was sent and no state changed.`,
    );
  }

  try {
    const response = await fetch(endpointUrl(endpoint), {
      method: "GET",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const payload: unknown = await response.json();
    if (!response.ok) {
      return buildReadError<T>(
        endpoint,
        `Read failed with HTTP ${response.status} ${response.statusText}`.trim(),
      );
    }
    if (!isApiEnvelope<T>(payload)) {
      return buildReadError<T>(
        endpoint,
        "Read failed because the API envelope is invalid.",
        "INTERNAL_ERROR",
      );
    }
    return payload;
  } catch (error) {
    return buildReadError<T>(
      endpoint,
      error instanceof Error ? error.message : "Read failed for an unknown reason.",
    );
  }
}

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
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.status)
    : resolveDemo(scenario, () => getDemoStatus(scenario));

export const getDrift = (
  scenario: DemoScenario,
  range: DriftRange = "24h",
): Promise<ApiEnvelope<DriftPayload>> =>
  isReadOnlyMode()
    ? readApi(`${API_ENDPOINTS.drift}?range=${encodeURIComponent(range)}`)
    : resolveDemo(scenario, () => getDemoDrift(scenario, range));

export const getTelemetry = (scenario: DemoScenario): Promise<ApiEnvelope<TelemetryPayload>> =>
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.telemetry)
    : resolveDemo(scenario, () => getDemoTelemetry(scenario));

export const getCanary = (scenario: DemoScenario): Promise<ApiEnvelope<CanaryPayload>> =>
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.canary)
    : resolveDemo(scenario, () => getDemoCanary(scenario));

export const getEvents = (scenario: DemoScenario): Promise<ApiEnvelope<EventsPayload>> =>
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.events)
    : resolveDemo(scenario, () => getDemoEvents(scenario));

export const getSafetyGates = (scenario: DemoScenario): Promise<ApiEnvelope<SafetyGatesPayload>> =>
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.safetyGates)
    : resolveDemo(scenario, () => getDemoSafetyGates(scenario));

export const getAudit = (scenario: DemoScenario): Promise<ApiEnvelope<AuditPayload>> =>
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.audit)
    : resolveDemo(scenario, () => getDemoAudit(scenario));

export const getCommands = (scenario: DemoScenario): Promise<ApiEnvelope<CommandsPayload>> =>
  isReadOnlyMode()
    ? readApi(API_ENDPOINTS.commands)
    : resolveDemo(scenario, () => getDemoCommands(scenario));

/* ------------------------------ STREAM ----------------------------- */

/**
 * Phase A stream adapter: permanently disconnected. It opens no EventSource,
 * emits no events, and never fabricates a connection. Phase B will implement
 * Last-Event-ID reconnect against API_ENDPOINTS.stream.
 */
export function createStreamAdapter(): StreamAdapter {
  if (isReadOnlyMode() && typeof EventSource !== "undefined") {
    const listeners = new Set<(event: StreamEvent) => void>();
    const eventSource = new EventSource(endpointUrl(API_ENDPOINTS.stream));
    let state: StreamAdapter["state"] = "CONNECTED";
    let lastEventId: string | undefined;

    eventSource.onmessage = (message) => {
      try {
        const event = JSON.parse(message.data) as StreamEvent;
        lastEventId = event.id;
        listeners.forEach((listener) => listener(event));
      } catch {
        const now = new Date().toISOString();
        listeners.forEach((listener) =>
          listener({
            type: "error",
            id: `stream-parse-error-${crypto.randomUUID()}`,
            at: now,
            payload: {
              code: "INTERNAL_ERROR",
              message: "Stream event could not be parsed.",
              retryable: true,
              occurredAt: now,
            },
          }),
        );
      }
    };
    eventSource.onerror = () => {
      state = "DEGRADED";
    };

    return {
      get state() {
        return state;
      },
      get lastEventId() {
        return lastEventId;
      },
      subscribe(listener) {
        listeners.add(listener);
        return () => listeners.delete(listener);
      },
      close() {
        state = "DISCONNECTED";
        listeners.clear();
        eventSource.close();
      },
    };
  }

  const listeners = new Set<(event: StreamEvent) => void>();
  return {
    state: "DISCONNECTED",
    lastEventId: undefined,
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
    API_MODE === "readonly" ? READONLY_COMMAND_BLOCK_LABEL : BACKEND_NOT_CONNECTED_LABEL,
    `${endpoint} is disabled in ${API_MODE} mode. No request was sent and no state changed.`,
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
