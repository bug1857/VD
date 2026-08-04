import { useResource } from "@/app/scenario";
import { PageHeader } from "@/components/AppShell";
import { LatencyChart, RecallChart, ThroughputChart } from "@/components/Charts";
import { formatNumber, OperationalPill, Pill } from "@/components/Indicators";
import { KeyValue, Mono, Panel } from "@/components/Panel";
import { Resource } from "@/components/StateViews";
import { getTelemetry } from "@/lib/api";

export function PerformancePage() {
  const telemetry = useResource(
    getTelemetry,
    (d) => d.recallSeries.length === 0 && d.latencySeries.length === 0 && d.slo.length === 0,
  );

  return (
    <>
      <PageHeader
        title="Retrieval Performance"
        description="Recall with lower confidence bound, mean / p95 / upper-bound latency, throughput and error rate, plus the objective matrix for the current evaluation windows."
      />

      <Resource
        state={telemetry.state}
        onRetry={telemetry.reload}
        loadingLabel="Reading telemetry"
        empty={{ title: "No telemetry", detail: "No samples were recorded in the active windows." }}
      >
        {(data) => (
          <div className="space-y-6">
            <div className="grid gap-4 sm:grid-cols-3">
              <div className="panel px-4 py-3.5">
                <span className="label-mono">Sample size</span>
                <p className="metric-value mt-2">
                  {data.sampleSize !== undefined ? data.sampleSize.toLocaleString("en-US") : "—"}
                </p>
              </div>
              <div className="panel px-4 py-3.5">
                <span className="label-mono">Windows</span>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {data.windows?.length ? (
                    data.windows.map((w) => <Pill key={w}>{w}</Pill>)
                  ) : (
                    <Mono>—</Mono>
                  )}
                </div>
              </div>
              <div className="panel px-4 py-3.5">
                <span className="label-mono">Confidence level</span>
                <p className="metric-value mt-2">
                  {data.confidenceLevel !== undefined
                    ? `${(data.confidenceLevel * 100).toFixed(0)}%`
                    : "—"}
                </p>
              </div>
            </div>

            <div className="grid gap-6 xl:grid-cols-2">
              <Panel
                title="Recall and lower confidence bound"
                subtitle="Solid: recall. Dashed: LCB. Demo data."
              >
                {data.recallSeries.length ? (
                  <RecallChart data={data.recallSeries} />
                ) : (
                  <p className="text-sm text-muted-foreground">No recall samples.</p>
                )}
              </Panel>
              <Panel
                title="Latency: mean, p95, UCB"
                subtitle="Milliseconds, per evaluation window."
              >
                {data.latencySeries.length ? (
                  <LatencyChart data={data.latencySeries} />
                ) : (
                  <p className="text-sm text-muted-foreground">No latency samples.</p>
                )}
              </Panel>
            </div>

            <div className="grid gap-6 xl:grid-cols-[1.3fr_1fr]">
              <Panel
                title="Throughput and errors"
                subtitle="Queries per second with observed error rate."
              >
                {data.throughputSeries.length ? (
                  <>
                    <ThroughputChart data={data.throughputSeries} />
                    <div className="mt-3 grid gap-4 sm:grid-cols-3">
                      <KeyValue
                        label="Peak qps"
                        value={
                          <Mono>
                            {formatNumber(Math.max(...data.throughputSeries.map((p) => p.qps)))}
                          </Mono>
                        }
                      />
                      <KeyValue
                        label="Latest error rate"
                        value={
                          <Mono>
                            {formatNumber(
                              (data.throughputSeries.at(-1)?.errorRate ?? 0) * 100,
                              "%",
                              2,
                            )}
                          </Mono>
                        }
                      />
                      <KeyValue
                        label="Points"
                        value={<Mono>{data.throughputSeries.length}</Mono>}
                      />
                    </div>
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">No throughput samples.</p>
                )}
              </Panel>

              <Panel title="SLO matrix" subtitle="Objective vs observed, per window.">
                <div className="overflow-x-auto">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Objective</th>
                        <th>Target</th>
                        <th>Observed</th>
                        <th>Window</th>
                        <th>State</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.slo.map((row) => (
                        <tr key={row.id}>
                          <td className="text-xs text-foreground">{row.objective}</td>
                          <td className="font-mono text-xs">{row.target ?? "—"}</td>
                          <td className="font-mono text-xs">{row.observed ?? "—"}</td>
                          <td className="font-mono text-xs">{row.window ?? "—"}</td>
                          <td>
                            <OperationalPill state={row.state} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Panel>
            </div>
          </div>
        )}
      </Resource>
    </>
  );
}
