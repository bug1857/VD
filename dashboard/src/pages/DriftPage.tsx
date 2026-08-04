import { useState } from "react";
import { useResource } from "@/app/scenario";
import { PageHeader } from "@/components/AppShell";
import { ContributionBars, MmdChart } from "@/components/Charts";
import { formatDateTime, formatNumber, OperationalPill, Pill } from "@/components/Indicators";
import { KeyValue, Mono, Panel } from "@/components/Panel";
import { Resource } from "@/components/StateViews";
import { getDrift } from "@/lib/api";
import type { DriftRange } from "@/lib/types";

const RANGES: DriftRange[] = ["1h", "6h", "24h", "7d"];

export function DriftPage() {
  const [range, setRange] = useState<DriftRange>("24h");
  const drift = useResource(
    (scenario) => getDrift(scenario, range),
    (d) => d.mmd2Series.length === 0 && d.ksRows.length === 0,
    [range],
  );

  return (
    <>
      <PageHeader
        title="Drift Intelligence"
        description="Kernel two-sample drift, per-feature KS tests with Holm correction, breach state, detector coverage, request-source composition and statistic attribution."
        aside={
          <div className="flex items-center gap-1.5" role="group" aria-label="Drift range">
            {RANGES.map((r) => (
              <button
                key={r}
                type="button"
                aria-pressed={range === r}
                onClick={() => setRange(r)}
                className={
                  range === r
                    ? "rounded-md border border-telemetry/50 bg-telemetry/10 px-2.5 py-1 font-mono text-xs text-telemetry"
                    : "btn-ghost"
                }
              >
                {r}
              </button>
            ))}
          </div>
        }
      />

      <Resource
        state={drift.state}
        onRetry={drift.reload}
        loadingLabel="Reading drift statistics"
        empty={{
          title: "No drift windows",
          detail: "No statistic was computed inside this range.",
        }}
      >
        {(data, envelope) => (
          <div className="space-y-6">
            <Panel
              title="MMD² over time"
              subtitle={`Range ${data.range}${data.threshold !== undefined ? ` · threshold ${data.threshold}` : ""} · Demo data`}
              actions={<Mono>{formatDateTime(envelope.freshness.observedAt)}</Mono>}
            >
              {data.mmd2Series.length > 0 ? (
                <MmdChart data={data.mmd2Series} threshold={data.threshold} />
              ) : (
                <p className="text-sm text-muted-foreground">No points in range.</p>
              )}
            </Panel>

            <div className="grid gap-6 xl:grid-cols-[1.4fr_1fr]">
              <Panel
                title="Per-feature KS / Holm"
                subtitle={
                  data.holmAlpha !== undefined
                    ? `α = ${data.holmAlpha}, Holm step-down adjusted`
                    : "Holm step-down adjusted"
                }
              >
                <div className="overflow-x-auto">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Feature</th>
                        <th>KS</th>
                        <th>p</th>
                        <th>Holm p</th>
                        <th>Decision</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.ksRows.map((row) => (
                        <tr key={row.feature}>
                          <td className="font-mono text-xs">{row.feature}</td>
                          <td className="font-mono text-xs">{row.ksStatistic.toFixed(3)}</td>
                          <td className="font-mono text-xs">{row.pValue.toExponential(2)}</td>
                          <td className="font-mono text-xs">
                            {row.holmAdjustedP !== undefined ? row.holmAdjustedP.toFixed(4) : "—"}
                          </td>
                          <td>
                            <Pill tone={row.rejected ? "warn" : "ok"}>
                              {row.rejected ? "REJECTED" : "RETAINED"}
                            </Pill>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Panel>

              <div className="space-y-6">
                <Panel title="Breach state">
                  {data.breach ? (
                    <div className="space-y-4">
                      <div className="flex items-center gap-2">
                        <OperationalPill state={data.breach.state} />
                        {data.breach.window && <Mono>{data.breach.window}</Mono>}
                      </div>
                      <div className="grid gap-4 sm:grid-cols-2">
                        <KeyValue
                          label="Detector"
                          value={<span className="text-xs">{data.breach.detector ?? "—"}</span>}
                        />
                        <KeyValue
                          label="Occurred at"
                          value={<Mono>{formatDateTime(data.breach.occurredAt)}</Mono>}
                        />
                        <KeyValue
                          label="Margin over threshold"
                          value={<Mono>{formatNumber(data.breach.margin)}</Mono>}
                        />
                      </div>
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      No breach reported for this range.
                    </p>
                  )}
                </Panel>

                <Panel title="Detector coverage">
                  {data.detectors?.length ? (
                    <ul className="space-y-2.5">
                      {data.detectors.map((d) => (
                        <li key={d.id} className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <p className="truncate text-sm text-foreground">{d.label}</p>
                            {d.detail && <Mono className="block">{d.detail}</Mono>}
                          </div>
                          <OperationalPill state={d.state} />
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="text-sm text-muted-foreground">No detectors reported.</p>
                  )}
                </Panel>
              </div>
            </div>

            <div className="grid gap-6 xl:grid-cols-2">
              <Panel
                title="Source composition"
                subtitle="Share of requests contributing to the evaluated window."
              >
                {data.sourceComposition?.length ? (
                  <ul className="space-y-3">
                    {data.sourceComposition.map((slice) => (
                      <li key={slice.label}>
                        <div className="flex items-baseline justify-between">
                          <span className="font-mono text-xs text-foreground">{slice.label}</span>
                          <span className="font-mono text-[11px] text-muted-foreground">
                            {(slice.share * 100).toFixed(1)}%
                          </span>
                        </div>
                        <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-surface-2">
                          <div
                            className="h-full rounded-full bg-violet/70"
                            style={{ width: `${slice.share * 100}%` }}
                          />
                        </div>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground">Composition not reported.</p>
                )}
              </Panel>

              <Panel
                title="Explainability"
                subtitle={data.explainability?.note ?? "Statistic attribution by feature."}
              >
                {data.explainability?.topContributors.length ? (
                  <ContributionBars items={data.explainability.topContributors} />
                ) : (
                  <p className="text-sm text-muted-foreground">
                    Attribution not available for this window.
                  </p>
                )}
              </Panel>
            </div>
          </div>
        )}
      </Resource>
    </>
  );
}
