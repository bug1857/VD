import { Activity, Clock } from "lucide-react";
import { useResource } from "@/app/scenario";
import { PageHeader } from "@/components/AppShell";
import {
  ConnectivityPill,
  EvidencePill,
  FreshnessPill,
  formatDateTime,
  formatNumber,
  formatTime,
  OperationalPill,
  Pill,
} from "@/components/Indicators";
import { MetricCard } from "@/components/MetricCard";
import { KeyValue, Mono, Panel } from "@/components/Panel";
import { Resource } from "@/components/StateViews";
import { getEvents, getStatus } from "@/lib/api";
import { DEMO_DATA_NOTICE } from "@/lib/demo-data";
import type { EventSeverity } from "@/lib/types";

const severityTone = (s: EventSeverity) =>
  s === "SUCCESS" ? "ok" : s === "WARNING" ? "warn" : s === "ERROR" ? "danger" : "info";

export function OverviewPage() {
  const status = useResource(getStatus, (d) => d.kpis.length === 0 && d.mode === "UNKNOWN");
  const events = useResource(getEvents, (d) => d.items.length === 0);

  return (
    <>
      <PageHeader
        title="Operational Overview"
        description="Single-screen read of tuning mode, system health, control-plane connectivity, active identities, and the newest recorded event. All figures are demo data."
        aside={
          status.state.kind === "ready" ? (
            <div className="flex items-center gap-2">
              <FreshnessPill freshness={status.state.envelope.freshness} />
              <Mono>{status.state.envelope.requestId}</Mono>
            </div>
          ) : undefined
        }
      />

      <Panel title="Status strip" subtitle={DEMO_DATA_NOTICE}>
        <Resource
          state={status.state}
          onRetry={status.reload}
          loadingLabel="Reading control-plane status"
        >
          {(data) => (
            <div className="space-y-5">
              <div className="flex flex-wrap items-center gap-2">
                <Pill tone="info">MODE {data.mode}</Pill>
                <OperationalPill state={data.health} />
                <ConnectivityPill state={data.connectivity.state} />
                {data.connectivity.detail && <Mono>{data.connectivity.detail}</Mono>}
              </div>

              <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                <KeyValue
                  label="Active config"
                  value={
                    data.config ? (
                      <span className="font-mono text-xs">
                        {data.config.label ?? data.config.id}
                      </span>
                    ) : (
                      "—"
                    )
                  }
                />
                <KeyValue
                  label="Dataset"
                  value={
                    data.dataset ? (
                      <span className="font-mono text-xs">
                        {data.dataset.label ?? data.dataset.id}
                      </span>
                    ) : (
                      "—"
                    )
                  }
                />
                <KeyValue
                  label="Last-known-good"
                  value={
                    data.lkg ? (
                      <span className="font-mono text-xs">
                        {data.lkg.config.label ?? data.lkg.config.id}
                      </span>
                    ) : (
                      "Not reported"
                    )
                  }
                />
                <KeyValue
                  label="Candidate"
                  value={
                    data.candidate ? (
                      <span className="font-mono text-xs">
                        {data.candidate.label ?? data.candidate.id}
                      </span>
                    ) : (
                      "None active"
                    )
                  }
                />
              </div>

              {data.latestEvent && (
                <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border/70 bg-surface-2/50 px-4 py-3">
                  <Pill tone={severityTone(data.latestEvent.severity)}>
                    {data.latestEvent.severity}
                  </Pill>
                  <span className="font-mono text-xs text-muted-foreground">
                    {data.latestEvent.kind}
                  </span>
                  <span className="text-sm text-foreground">{data.latestEvent.message}</span>
                  <span className="ml-auto flex items-center gap-1.5 text-xs text-muted-foreground">
                    <Clock className="h-3.5 w-3.5" aria-hidden />
                    {formatDateTime(data.latestEvent.at)}
                  </span>
                </div>
              )}
            </div>
          )}
        </Resource>
      </Panel>

      <Resource state={status.state} onRetry={status.reload} loadingLabel="Reading KPIs">
        {(data) => (
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {data.kpis.map((kpi) => (
              <MetricCard
                key={kpi.id}
                label={kpi.label}
                value={formatNumber(kpi.value)}
                unit={kpi.unit}
                state={kpi.state}
                detail={kpi.detail}
                threshold={kpi.threshold}
              />
            ))}
          </div>
        )}
      </Resource>

      <div className="grid gap-6 xl:grid-cols-[1.6fr_1fr]">
        <Panel
          title="Event timeline"
          subtitle="Newest first. Backfilled from the events endpoint; the live stream adapter is disconnected in Phase A."
        >
          <Resource
            state={events.state}
            onRetry={events.reload}
            loadingLabel="Reading events"
            empty={{
              title: "No events recorded",
              detail: "The event window is empty. Nothing is synthesized.",
            }}
          >
            {(data) => (
              <ol className="relative space-y-3 border-l border-border/70 pl-5">
                {data.items.map((item) => (
                  <li key={item.id} className="relative">
                    <span
                      aria-hidden
                      className="absolute -left-[1.4rem] top-1.5 h-2 w-2 rounded-full border border-border bg-surface-3"
                    />
                    <div className="flex flex-wrap items-center gap-2">
                      <Pill tone={severityTone(item.severity)}>{item.severity}</Pill>
                      <span className="font-mono text-[11px] text-muted-foreground">
                        {item.kind}
                      </span>
                      <span className="ml-auto font-mono text-[11px] text-muted-foreground">
                        {formatTime(item.at)}
                      </span>
                    </div>
                    <p className="mt-1 text-sm text-foreground">{item.message}</p>
                    {(item.source || item.configId) && (
                      <Mono className="mt-0.5 block">
                        {[item.source, item.configId].filter(Boolean).join(" · ")}
                      </Mono>
                    )}
                  </li>
                ))}
              </ol>
            )}
          </Resource>
        </Panel>

        <div className="space-y-6">
          <Panel title="Freshness & connectivity">
            <Resource state={status.state} onRetry={status.reload} loadingLabel="Reading freshness">
              {(data, envelope) => (
                <div className="space-y-4">
                  <div className="flex flex-wrap items-center gap-2">
                    <FreshnessPill freshness={envelope.freshness} />
                    <ConnectivityPill state={envelope.connectivity.state} />
                  </div>
                  <div className="grid gap-4 sm:grid-cols-2">
                    <KeyValue
                      label="Observed at"
                      value={<Mono>{formatDateTime(envelope.freshness.observedAt)}</Mono>}
                    />
                    <KeyValue
                      label="TTL"
                      value={
                        <Mono>
                          {envelope.freshness.ttlSeconds !== undefined
                            ? `${envelope.freshness.ttlSeconds}s`
                            : "—"}
                        </Mono>
                      }
                    />
                    <KeyValue
                      label="Endpoint"
                      value={<Mono>{envelope.connectivity.endpoint ?? "—"}</Mono>}
                    />
                    <KeyValue
                      label="Last contact"
                      value={<Mono>{formatDateTime(envelope.connectivity.lastContactAt)}</Mono>}
                    />
                  </div>
                  {data.outbox && (
                    <div className="rounded-lg border border-border/70 bg-surface-2/40 px-3 py-2.5">
                      <div className="flex items-center justify-between">
                        <span className="label-mono">Outbox</span>
                        <OperationalPill state={data.outbox.state} />
                      </div>
                      <Mono className="mt-1.5 block">
                        pending {data.outbox.pending ?? "—"} · in-flight{" "}
                        {data.outbox.inFlight ?? "—"} · failed {data.outbox.failed ?? "—"}
                      </Mono>
                    </div>
                  )}
                </div>
              )}
            </Resource>
          </Panel>

          <Panel
            title="Evidence vocabulary"
            subtitle="Statuses used across canary and audit views."
          >
            <div className="flex flex-wrap gap-2">
              <EvidencePill status="FRESHLY VERIFIED" />
              <EvidencePill status="REPRODUCIBLE BUT NOT EXECUTED" />
              <EvidencePill status="HISTORICAL ONLY" />
              <EvidencePill status="CONTRADICTED" />
              <EvidencePill status="UNVERIFIABLE" />
              <EvidencePill status="BLOCKED" />
            </div>
            <p className="mt-3 flex items-start gap-2 text-xs text-muted-foreground">
              <Activity className="mt-0.5 h-3.5 w-3.5 shrink-0 text-telemetry" aria-hidden />
              Nothing in this interface claims success it has not observed. Absent fields render as
              “—”.
            </p>
          </Panel>
        </div>
      </div>
    </>
  );
}
