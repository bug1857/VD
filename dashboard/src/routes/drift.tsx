import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { PageShell, Block, KeyRow, Prose } from "@/components/vd/page";
import { driftWindows } from "@/lib/vd-data";

export const Route = createFileRoute("/drift")({
  head: () => ({
    meta: [
      { title: "Drift Intelligence — VD Control Center" },
      {
        name: "description",
        content:
          "Workload regime, MMD² statistics against threshold, Holm-adjusted decisions and breach chronology for the current vector workload.",
      },
      { property: "og:title", content: "Drift Intelligence — VD Control Center" },
      {
        property: "og:description",
        content:
          "MMD² evidence, Holm-adjusted drift decisions and breach chronology for the current vector workload.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: DriftPage,
});

function DriftPage() {
  const [sel, setSel] = useState<string>(driftWindows[0]!.id);
  const w = driftWindows.find((d) => d.id === sel)!;

  const series = [...driftWindows].reverse();
  const max = 0.028;
  const W = 1000;
  const H = 190;
  const x = (i: number) => (i / (series.length - 1)) * (W - 40) + 20;
  const y = (v: number) => H - 24 - (v / max) * (H - 48);
  const thr = y(series[0]!.threshold);

  return (
    <PageShell
      eyebrow="Drift intelligence"
      title="Regime unchanged — L2 · target-075."
      lede="Twelve consecutive observation windows were evaluated with a kernel two-sample test. No window produced a Holm-adjusted breach, so the workload regime is carried forward unchanged."
      facts={[
        { k: "regime", v: "L2 · target-075" },
        { k: "test", v: "MMD² · RBF kernel" },
        { k: "threshold", v: "0.0210" },
        { k: "correction", v: "Holm · m 12" },
        { k: "last breach", v: "none in window" },
        { k: "evaluated", v: "13:18:02Z" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <Block
          title="Breach chronology"
          note="MMD² per window against the regime threshold. Evidence, not decision."
        >
          <div className="tonal -mx-6 rounded-md px-6 py-6">
            <svg
              viewBox={`0 0 ${W} ${H}`}
              className="h-[190px] w-full"
              role="img"
              aria-label="MMD squared per observation window relative to the drift threshold"
            >
              <line
                x1={20}
                x2={W - 20}
                y1={thr}
                y2={thr}
                stroke="var(--blocked)"
                strokeWidth={1}
                strokeDasharray="3 5"
                opacity={0.5}
              />
              <text
                x={W - 20}
                y={thr - 7}
                textAnchor="end"
                className="mono"
                fontSize="11"
                fill="var(--text-disabled)"
              >
                threshold 0.0210
              </text>
              <polyline
                points={series.map((d, i) => `${x(i)},${y(d.mmd2)}`).join(" ")}
                fill="none"
                stroke="var(--text-secondary)"
                strokeWidth={1.1}
                opacity={0.75}
              />
              {series.map((d, i) => {
                const active = d.id === sel;
                return (
                  <g
                    key={d.id}
                    onMouseEnter={() => setSel(d.id)}
                    className="cursor-pointer"
                  >
                    <rect
                      x={x(i) - 18}
                      y={10}
                      width={36}
                      height={H - 20}
                      fill="transparent"
                    />
                    <line
                      x1={x(i)}
                      x2={x(i)}
                      y1={y(d.mmd2)}
                      y2={H - 24}
                      stroke="var(--border-strong)"
                      strokeWidth={1}
                      opacity={active ? 1 : 0.5}
                    />
                    <circle
                      cx={x(i)}
                      cy={y(d.mmd2)}
                      r={active ? 3.4 : 2.2}
                      fill={
                        d.decision === "inconclusive"
                          ? "var(--attention)"
                          : "var(--text-primary)"
                      }
                      opacity={active ? 1 : 0.7}
                    />
                    <text
                      x={x(i)}
                      y={H - 8}
                      textAnchor="middle"
                      className="mono"
                      fontSize="10.5"
                      fill={
                        active ? "var(--text-secondary)" : "var(--text-disabled)"
                      }
                    >
                      {d.time}
                    </text>
                  </g>
                );
              })}
            </svg>
          </div>
        </Block>

        <div className="mt-12 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1.25fr)_minmax(0,0.75fr)] xl:gap-20">
          <Block title="Window evidence" note="12 windows · most recent first">
            <div>
              <div className="grid grid-cols-[80px_110px_110px_90px_90px_1fr] gap-x-6 pb-2 text-[12px] text-ink-4">
                <span>window</span>
                <span className="text-right">MMD²</span>
                <span className="text-right">threshold</span>
                <span className="text-right">p</span>
                <span className="text-right">Holm p</span>
                <span className="text-right">decision</span>
              </div>
              {driftWindows.map((d) => (
                <button
                  key={d.id}
                  type="button"
                  onMouseEnter={() => setSel(d.id)}
                  onClick={() => setSel(d.id)}
                  className={[
                    "grid w-full grid-cols-[80px_110px_110px_90px_90px_1fr] items-baseline gap-x-6 border-t border-line py-2.5 text-left transition-colors duration-150",
                    d.id === sel ? "bg-hover/50" : "hover:bg-hover/35",
                  ].join(" ")}
                >
                  <span className="mono text-[12.5px] text-ink-3">{d.time}</span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink">
                    {d.mmd2.toFixed(4)}
                  </span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink-4">
                    {d.threshold.toFixed(4)}
                  </span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink-2">
                    {d.p.toFixed(3)}
                  </span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink-2">
                    {d.holm.toFixed(3)}
                  </span>
                  <span
                    className={[
                      "text-right text-[12.5px]",
                      d.decision === "inconclusive"
                        ? "text-attention"
                        : "text-ink-3",
                    ].join(" ")}
                  >
                    {d.decision}
                  </span>
                </button>
              ))}
            </div>
          </Block>

          <Block title="Selected window" note={w.id}>
            <div className="tonal -mx-5 rounded-md px-5 py-4">
              <KeyRow k="observed at" v={`2026-08-09 ${w.time}:00Z`} />
              <KeyRow k="MMD² statistic" v={w.mmd2.toFixed(4)} />
              <KeyRow k="regime threshold" v={w.threshold.toFixed(4)} />
              <KeyRow k="raw p-value" v={w.p.toFixed(3)} />
              <KeyRow
                k="Holm-adjusted p"
                v={w.holm.toFixed(3)}
                sub="family-wise · m 12"
              />
              <KeyRow
                k="decision"
                v={w.decision}
                tone={w.decision === "inconclusive" ? "attention" : "muted"}
                mono={false}
              />
              <KeyRow k="regime action" v="carry forward" mono={false} />
            </div>
            <div className="mt-6">
              <Prose>
                A drift decision is evidence about the workload. It does not
                qualify a configuration, does not admit a candidate, and does
                not authorize routing. A Holm-adjusted breach would open a
                re-qualification path only; activation would still require a
                signed grant.
              </Prose>
            </div>
          </Block>
        </div>
      </div>
    </PageShell>
  );
}
