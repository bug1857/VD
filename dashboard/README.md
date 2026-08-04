# VD Control Center

Operator dashboard for **Adaptive Milvus Tuning • Drift Detection • Canary Operations**.

This repository is **Phase A: a client-side React + TypeScript + Vite application**.
It contains no server code, no SSR, no database, no authentication, and no access to
Milvus. By default, every number, identity, chart point, audit record and safety gate
rendered by this app comes from a typed demo snapshot in `src/lib/demo-data.ts` and is
visibly labelled **Demo data** in the UI.

An optional read-only integration mode can read VD telemetry envelopes from a local
`/api/v1` service. It never enables approval, routing, rollback, export, or any other
command path.

Treat this project root as the future `dashboard/` directory of the VD repository.

## Setup

```bash
bun install --frozen-lockfile
bun run dev      # http://127.0.0.1:8080
bun run build    # typecheck + production build to dist/
bun run lint     # ESLint + Prettier
bun test         # Phase-A API safety boundary
bun run preview  # serve the production build locally
```

The Vite development and preview servers bind only to `127.0.0.1`; this Phase-A
dashboard is not exposed to the local network.

## Read-only VD API mode

Run the dashboard against a local read-only VD API service:

```bash
VITE_VD_API_MODE=readonly VITE_VD_API_BASE=http://127.0.0.1:8081 bun run dev
```

If `VITE_VD_API_BASE` is omitted, reads use same-origin `/api/v1/*` paths.

Only these paths are read:

```text
GET /api/v1/status
GET /api/v1/drift?range=1h|6h|24h|7d
GET /api/v1/telemetry
GET /api/v1/canary
GET /api/v1/events
GET /api/v1/safety-gates
GET /api/v1/audit
GET /api/v1/commands
GET /api/v1/stream
```

All command functions remain locally blocked in both modes:

```text
POST /api/v1/commands/verify-grant
POST /api/v1/commands/start-canary
POST /api/v1/commands/pause-routing
POST /api/v1/commands/request-rollback
POST /api/v1/commands/export-audit
```

Stack: Vite 8, React 19, TypeScript (strict, `exactOptionalPropertyTypes`),
Tailwind CSS v4, Recharts, Lucide.

## Structure

```
index.html                 SPA entry, static head metadata
src/main.tsx               React root
src/app/App.tsx            Route table, per-view document title/description
src/app/router.tsx         Minimal History-API client router (no server routes)
src/app/scenario.tsx       Demo scenario context + useResource state machine
src/components/            AppShell, Panel, MetricCard, Indicators, Charts, StateViews, NavLink
src/pages/                 The six views
src/lib/types.ts           All API envelopes, domain types, SSE discriminated union, error types
src/lib/api.ts             All endpoint constants and the only call surface the UI may use
src/lib/demo-data.ts       Single source of truth for every displayed value
src/styles.css             Dark control-room design system (OKLCH tokens)
```

## Views

| View               | Path              | Contents                                                                                                                        |
| ------------------ | ----------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Overview           | `/`               | Mode / health / connectivity / identities / latest event strip, KPI grid, event timeline, freshness                             |
| Drift Intelligence | `/drift`          | MMD² series vs threshold, KS/Holm table, breach history, detector + source composition, explainability, range controls          |
| Performance        | `/performance`    | Recall with lower confidence bound, mean/p95 latency with upper bound, throughput, errors, SLO matrix, sample and window counts |
| Canary Operations  | `/canary`         | Data-derived LKG/candidate split, candidate deltas, evidence, confidence, recall/latency, reason, lifecycle, schedule, outbox   |
| Command Center     | `/command-center` | Verify grant, start canary, pause routing, request rollback, export audit; safety gates; command history                        |
| Audit & Evidence   | `/audit`          | Searchable audit table, payload hashes, verification status, experiments, source revision                                       |

## Demo guarantee

- Every displayed value is centralized in `src/lib/demo-data.ts` and marked **Demo data**.
- Components never hard-code `ef`, `M`, `nprobe`, candidate count, traffic split, or the
  last-known-good identity. All of it arrives from typed payloads; absent optional fields
  render as `—`, never as a fabricated value.
- Nothing is persisted. No `localStorage`, no cookies, no writes of any kind.
- Nothing claims success it has not observed. Command buttons are disabled and read
  **Backend not connected**; the confirmation modal previews the exact target, config
  identity, dataset identity, reason, typed confirmation phrase and blast-radius warning,
  but its submit control stays disabled.
- Command history supports `queued | accepted | rejected | completed | failed` but is
  empty by construction in Phase A, because no command is ever submitted.
- Audit export is presented as a backend-only capability and is disabled.
- The stream adapter is a disconnected no-op: it opens nothing and emits nothing.

### Demo scenarios

A global scenario selector in the header drives every read through the same
`useResource` state machine, so each state is explicit, accessible and styled:

`normal` · `loading` · `stale` · `disconnected` · `blocked` · `unauthorized` · `empty` · `error`

## Endpoints

All strings live in `API_ENDPOINTS` (`src/lib/api.ts`). No component builds a URL.

| Method | Endpoint                            | Phase A behaviour                                 |
| ------ | ----------------------------------- | ------------------------------------------------- |
| GET    | `/api/v1/status`                    | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/drift`                     | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/telemetry`                 | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/canary`                    | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/events`                    | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/safety-gates`              | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/audit`                     | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/commands`                  | Demo snapshot, or read-only fetch when enabled    |
| GET    | `/api/v1/stream`                    | Disconnected no-op, or read-only SSE when enabled |
| POST   | `/api/v1/commands/verify-grant`     | No fetch; throws locally                          |
| POST   | `/api/v1/commands/start-canary`     | No fetch; throws locally                          |
| POST   | `/api/v1/commands/pause-routing`    | No fetch; throws locally                          |
| POST   | `/api/v1/commands/request-rollback` | No fetch; throws locally                          |
| POST   | `/api/v1/commands/export-audit`     | No fetch; throws locally                          |

## Later write-capable integration with the VD Python service

The current integration is read-only. A later write-capable phase may replace only the
command bodies inside `src/lib/api.ts`. Endpoint constants, method names, envelope
shapes and every component should stay untouched.

1. **REST reads.** Each GET becomes `fetch(API_ENDPOINTS.x)` returning the same
   `ApiEnvelope<T>`. The service is expected to emit the envelope verbatim:
   `ok`, `data`, `error`, `freshness`, `identity`, `source_revision`.
2. **SSE stream.** `openStream()` becomes an `EventSource` on `/api/v1/stream`,
   parsed into the `StreamEvent` discriminated union in `src/lib/types.ts`.
   Reconnects must send `Last-Event-ID`; the server replays from that id so the
   timeline cannot silently skip events. Until a stream event is received, the UI
   keeps showing the freshness of the last completed REST read.
3. **Commands.** Each POST replaces its throw with a real request. A `202` means
   **queued, not success**: the UI must show `queued` and only advance to
   `accepted`, `rejected`, `completed` or `failed` when the service reports that
   transition (via `/api/v1/commands` or a stream event). No optimistic success.

### Envelope, freshness and identity semantics

- **Freshness.** `observed_at` + `ttl_seconds` classify a read as `FRESH`, `STALE`
  or `UNKNOWN`. Stale reads stay visible but are labelled stale; they never look fresh.
- **Identity.** Config identity, dataset identity and experiment id travel with the
  data. Any control action names the exact identities it would target, so an operator
  never acts against an assumed configuration.
- **Evidence vocabulary.** `FRESHLY VERIFIED`, `REPRODUCIBLE BUT NOT EXECUTED`,
  `HISTORICAL ONLY`, `CONTRADICTED`, `UNVERIFIABLE`, `BLOCKED` — used exactly as written.
- **Safety gates.** Required gates must pass before submission can ever be enabled.
  In Phase A the operator-grant gate fails by definition (the backend is not connected),
  so no action is enabled.
- **Errors.** Typed codes (`BACKEND_NOT_CONNECTED`, `UNAUTHORIZED`, `BLOCKED_BY_GATE`,
  `STALE_DATA`, `UPSTREAM_ERROR`, …) drive the explicit UI states rather than generic toasts.

## Isolation

This dashboard has no dependency on, and no access to, the VD Python packages,
FastAPI services, tuning workers, or the Milvus deployment. It reads nothing and
writes nothing outside this directory. Backend behaviour is only ever described,
never simulated as if it happened.
