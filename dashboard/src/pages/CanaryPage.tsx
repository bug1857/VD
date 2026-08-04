import { useResource } from "@/app/scenario";
import { PageHeader } from "@/components/AppShell";
import {
  EvidencePill,
  formatDateTime,
  formatDelta,
  formatNumber,
  LifecyclePill,
  OperationalPill,
  Pill,
} from "@/components/Indicators";
import { KeyValue, Mono, Panel } from "@/components/Panel";
import { Resource } from "@/components/StateViews";
import { getCanary } from "@/lib/api";
import type { TrafficSplit } from "@/lib/types";

const ROLE_COLOR: Record<"LKG" | "CANDIDATE" | "OTHER", string> = {
  LKG: "bg-ok/70",
  CANDIDATE: "bg-telemetry/70",
  OTHER: "bg-violet/70",
};

function SplitBar({ split }: { split: TrafficSplit }) {
  const total = split.slices.reduce((sum, s) => sum + s.percent, 0) || 1;
  return (
    <div className="space-y-3">
      <div className="flex h-3 w-full overflow-hidden rounded-full border border-border/70 bg-surface-2">
        {split.slices.map((slice) => (
          <div
            key={slice.identityId}
            className={ROLE_COLOR[slice.role]}
            style={{ width: `${(slice.percent / total) * 100}%` }}
            title={`${slice.label ?? slice.identityId}: ${slice.percent}%`}
          />
        ))}
      </div>
      <ul className="grid gap-3 sm:grid-cols-2">
        {split.slices.map((slice) => (
          <li
            key={slice.identityId}
            className="rounded-lg border border-border/70 bg-surface-2/40 px-3 py-2.5"
          >
            <div className="flex items-center justify-between gap-2">
              <Pill
                tone={slice.role === "LKG" ? "ok" : slice.role === "CANDIDATE" ? "info" : "neutral"}
              >
                {slice.role}
              </Pill>
              <span className="font-mono text-sm text-foreground">{slice.percent}%</span>
            </div>
            <Mono className="mt-1.5 block truncate">{slice.label ?? slice.identityId}</Mono>
            <Mono className="block truncate">{slice.identityId}</Mono>
          </li>
        ))}
      </ul>
      <Mono>
        allocation source: {split.source ?? "not reported"} · updated{" "}
        {formatDateTime(split.updatedAt)}
      </Mono>
    </div>
  );
}

export function CanaryPage() {
  const canary = useResource(
    getCanary,
    (d) => d.lifecycle.length === 0 && !d.candidate && !d.split,
  );

  return (
    <>
      <PageHeader
        title="Canary Operations"
        description="Traffic allocation, candidate evidence and the full promotion lifecycle. Every identity, allocation and count below is read from the canary payload — none are hard-coded in the interface."
      />

      <Resource
        state={canary.state}
        onRetry={canary.reload}
        loadingLabel="Reading canary state"
        empty={{
          title: "No canary activity",
          detail: "No allocation, candidate or lifecycle record was reported.",
        }}
      >
        {(data) => (
          <div className="space-y-6">
            <div className="grid gap-6 xl:grid-cols-[1.2fr_1fr]">
              <Panel
                title="Traffic allocation"
                subtitle="Derived entirely from the reported routing split."
              >
                {data.split && data.split.slices.length > 0 ? (
                  <SplitBar split={data.split} />
                ) : (
                  <p className="text-sm text-muted-foreground">No routing split reported.</p>
                )}
              </Panel>

              <Panel title="Last-known-good" subtitle="Rollback target identity.">
                {data.lkg ? (
                  <div className="grid gap-4 sm:grid-cols-2">
                    <KeyValue
                      label="Config"
                      value={<Mono>{data.lkg.config.label ?? data.lkg.config.id}</Mono>}
                    />
                    <KeyValue label="Identity" value={<Mono>{data.lkg.config.id}</Mono>} />
                    <KeyValue
                      label="Revision"
                      value={<Mono>{data.lkg.config.revision ?? "—"}</Mono>}
                    />
                    <KeyValue label="Hash" value={<Mono>{data.lkg.config.hash ?? "—"}</Mono>} />
                    <KeyValue
                      label="Promoted at"
                      value={<Mono>{formatDateTime(data.lkg.promotedAt)}</Mono>}
                    />
                    <KeyValue
                      label="Recall at promotion"
                      value={<Mono>{formatNumber(data.lkg.recall)}</Mono>}
                    />
                    {data.lkg.config.params && (
                      <div className="sm:col-span-2">
                        <span className="label-mono">Reported parameters</span>
                        <div className="mt-1.5 flex flex-wrap gap-1.5">
                          {Object.entries(data.lkg.config.params).map(([k, v]) => (
                            <Pill key={k} tone="neutral" className="normal-case">
                              {k}={String(v)}
                            </Pill>
                          ))}
                        </div>
                      </div>
                    )}
                    {data.lkg.note && (
                      <p className="text-xs text-muted-foreground sm:col-span-2">{data.lkg.note}</p>
                    )}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    No last-known-good identity reported.
                  </p>
                )}
              </Panel>
            </div>

            <Panel
              title="Candidate under evaluation"
              subtitle={data.candidate?.reason ?? "No reason reported."}
              actions={
                data.candidate?.candidateCount !== undefined ? (
                  <Pill tone="info">{data.candidate.candidateCount} candidates in set</Pill>
                ) : undefined
              }
            >
              {data.candidate ? (
                <div className="space-y-5">
                  <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                    <KeyValue
                      label="Config"
                      value={<Mono>{data.candidate.config.label ?? data.candidate.config.id}</Mono>}
                    />
                    <KeyValue
                      label="Confidence"
                      value={<Mono>{formatNumber(data.candidate.confidence)}</Mono>}
                    />
                    <KeyValue
                      label="Recall / LCB"
                      value={
                        <Mono>
                          {formatNumber(data.candidate.recall)} /{" "}
                          {formatNumber(data.candidate.recallLcb)}
                        </Mono>
                      }
                    />
                    <KeyValue
                      label="p95 latency"
                      value={<Mono>{formatNumber(data.candidate.p95Ms, "ms", 1)}</Mono>}
                    />
                  </div>

                  <div className="grid gap-6 xl:grid-cols-2">
                    <div className="overflow-x-auto">
                      <table className="data-table">
                        <thead>
                          <tr>
                            <th>Metric</th>
                            <th>Baseline</th>
                            <th>Candidate</th>
                            <th>Δ</th>
                            <th>State</th>
                          </tr>
                        </thead>
                        <tbody>
                          {data.candidate.deltas.map((d) => (
                            <tr key={d.metric}>
                              <td className="text-xs text-foreground">{d.metric}</td>
                              <td className="font-mono text-xs">
                                {formatNumber(d.baseline, d.unit)}
                              </td>
                              <td className="font-mono text-xs">
                                {formatNumber(d.candidate, d.unit)}
                              </td>
                              <td className="font-mono text-xs">{formatDelta(d.delta, d.unit)}</td>
                              <td>
                                <OperationalPill state={d.state} />
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    <ul className="space-y-2.5">
                      {data.candidate.evidence.map((ev) => (
                        <li
                          key={ev.id}
                          className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border/70 bg-surface-2/40 px-3 py-2.5"
                        >
                          <div className="min-w-0">
                            <p className="truncate text-sm text-foreground">{ev.label}</p>
                            {ev.detail && <Mono className="block">{ev.detail}</Mono>}
                          </div>
                          <EvidencePill status={ev.status} />
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">No candidate is under evaluation.</p>
              )}
            </Panel>

            <div className="grid gap-6 xl:grid-cols-[1.4fr_1fr]">
              <Panel
                title="Promotion lifecycle"
                subtitle="Detection through promotion or rollback."
              >
                {data.lifecycle.length ? (
                  <ol className="space-y-3">
                    {data.lifecycle.map((stage, i) => (
                      <li key={stage.id} className="flex items-start gap-3">
                        <span className="mt-0.5 font-mono text-[11px] text-muted-foreground">
                          {String(i + 1).padStart(2, "0")}
                        </span>
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="text-sm text-foreground">{stage.label}</span>
                            <LifecyclePill state={stage.state} />
                            <Mono className="ml-auto">{formatDateTime(stage.at)}</Mono>
                          </div>
                          {stage.detail && <Mono className="mt-0.5 block">{stage.detail}</Mono>}
                        </div>
                      </li>
                    ))}
                  </ol>
                ) : (
                  <p className="text-sm text-muted-foreground">No lifecycle stages reported.</p>
                )}
              </Panel>

              <div className="space-y-6">
                <Panel title="Schedule">
                  <div className="grid gap-4 sm:grid-cols-2">
                    <KeyValue
                      label="Next evaluation"
                      value={<Mono>{formatDateTime(data.schedule?.nextEvaluationAt)}</Mono>}
                    />
                    <KeyValue
                      label="Cadence"
                      value={<Mono>{data.schedule?.cadence ?? "—"}</Mono>}
                    />
                    <KeyValue label="Window" value={<Mono>{data.schedule?.window ?? "—"}</Mono>} />
                    <KeyValue
                      label="Hold-down"
                      value={
                        <Mono>
                          {data.schedule?.holdDownSeconds !== undefined
                            ? `${data.schedule.holdDownSeconds}s`
                            : "—"}
                        </Mono>
                      }
                    />
                  </div>
                </Panel>
                <Panel title="Outbox">
                  {data.outbox ? (
                    <div className="space-y-3">
                      <OperationalPill state={data.outbox.state} />
                      <div className="grid gap-4 sm:grid-cols-2">
                        <KeyValue
                          label="Pending"
                          value={<Mono>{data.outbox.pending ?? "—"}</Mono>}
                        />
                        <KeyValue
                          label="In flight"
                          value={<Mono>{data.outbox.inFlight ?? "—"}</Mono>}
                        />
                        <KeyValue label="Failed" value={<Mono>{data.outbox.failed ?? "—"}</Mono>} />
                        <KeyValue
                          label="Oldest pending"
                          value={
                            <Mono>
                              {data.outbox.oldestPendingAgeSeconds !== undefined
                                ? `${data.outbox.oldestPendingAgeSeconds}s`
                                : "—"}
                            </Mono>
                          }
                        />
                      </div>
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">Outbox not reported.</p>
                  )}
                </Panel>
              </div>
            </div>
          </div>
        )}
      </Resource>
    </>
  );
}
