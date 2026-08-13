import { createFileRoute } from "@tanstack/react-router";
import { PageShell, Block, KeyRow, Prose } from "@/components/vd/page";
import { ResponseProfile } from "@/components/vd/ResponseProfile";
import { responseProfile } from "@/lib/vd-data";

export const Route = createFileRoute("/response")({
  head: () => ({
    meta: [
      { title: "Response Intelligence — VD Control Center" },
      {
        name: "description",
        content:
          "Predicted recall and p95 latency across HNSW ef 200–1600 with uncertainty bands, tradeoff geometry and evidence applicability.",
      },
      {
        property: "og:title",
        content: "Response Intelligence — VD Control Center",
      },
      {
        property: "og:description",
        content:
          "Predicted recall/latency response profile across ef 200–1600 with uncertainty bands. Prediction only.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: ResponsePage,
});

const W = 940;
const H = 380;
const L_MIN = 0;
const L_MAX = 72;
const R_MIN = 0.85;
const R_MAX = 1.0;

function ResponsePage() {
  const x = (ms: number) => 60 + ((ms - L_MIN) / (L_MAX - L_MIN)) * (W - 100);
  const y = (r: number) => H - 46 - ((r - R_MIN) / (R_MAX - R_MIN)) * (H - 86);

  return (
    <PageShell
      eyebrow="Response intelligence"
      title="Predicted response across the permitted ef ladder."
      lede="Capped recall and p95 latency were estimated for ef 200, 400, 800 and 1600 from a single closed observation window. Every value on this page is a prediction with an interval — it establishes no configuration and authorizes nothing."
      facts={[
        { k: "observations", v: "1,200" },
        { k: "estimator", v: "bootstrap-2k" },
        { k: "interval", v: "95% LCB–UCB" },
        { k: "profile digest", v: "a83e5c17…19" },
        { k: "evidence age", v: "14 min" },
        { k: "applicability", v: "applicable", tone: "verified" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <Block
          title="Recall / latency tradeoff"
          note="Predicted mean with uncertainty rectangle per ef. Prediction only — carries no authority to route."
        >
          <div className="tonal -mx-6 rounded-md px-6 py-6">
            <svg
              viewBox={`0 0 ${W} ${H}`}
              className="h-[380px] w-full"
              role="img"
              aria-label="Predicted capped recall against p95 latency for each ef value, with uncertainty rectangles"
            >
              {[0.85, 0.9, 0.95, 1.0].map((r) => (
                <g key={r}>
                  <line x1={60} x2={W - 40} y1={y(r)} y2={y(r)} stroke="var(--border-subtle)" />
                  <text
                    x={50}
                    y={y(r) + 3.5}
                    textAnchor="end"
                    className="mono"
                    fontSize="11"
                    fill="var(--text-disabled)"
                  >
                    {r.toFixed(2)}
                  </text>
                </g>
              ))}
              {[0, 18, 36, 54, 72].map((ms) => (
                <text
                  key={ms}
                  x={x(ms)}
                  y={H - 22}
                  textAnchor="middle"
                  className="mono"
                  fontSize="11"
                  fill="var(--text-disabled)"
                >
                  {ms} ms
                </text>
              ))}

              <polyline
                points={responseProfile.map((p) => `${x(p.p95)},${y(p.recall)}`).join(" ")}
                fill="none"
                stroke="var(--text-disabled)"
                strokeWidth={1}
                strokeDasharray="2 5"
              />

              {responseProfile.map((p) => {
                const serving = p.role === "serving";
                const candidate = p.role === "candidate";
                const stroke = candidate
                  ? "var(--accent)"
                  : serving
                    ? "var(--text-primary)"
                    : "var(--text-tertiary)";
                return (
                  <g key={p.ef}>
                    <rect
                      x={x(p.p95Lcb)}
                      y={y(p.recallUcb)}
                      width={x(p.p95Ucb) - x(p.p95Lcb)}
                      height={y(p.recallLcb) - y(p.recallUcb)}
                      fill={stroke}
                      opacity={candidate ? 0.1 : 0.07}
                      stroke={stroke}
                      strokeOpacity={candidate ? 0.4 : 0.22}
                      strokeDasharray={candidate ? "3 4" : undefined}
                    />
                    <circle
                      cx={x(p.p95)}
                      cy={y(p.recall)}
                      r={serving ? 3.6 : 2.8}
                      fill={stroke}
                      opacity={serving ? 1 : 0.85}
                    />
                    <text
                      x={x(p.p95)}
                      y={y(p.recall) - 12}
                      textAnchor="middle"
                      className="mono"
                      fontSize="12"
                      fill={
                        serving
                          ? "var(--text-primary)"
                          : candidate
                            ? "var(--accent)"
                            : "var(--text-tertiary)"
                      }
                    >
                      ef {p.ef}
                    </text>
                    {(serving || candidate) && (
                      <text
                        x={x(p.p95)}
                        y={y(p.recall) + 22}
                        textAnchor="middle"
                        fontSize="11"
                        fill="var(--text-disabled)"
                      >
                        {serving ? "serving · LKG" : "candidate · predicted"}
                      </text>
                    )}
                  </g>
                );
              })}

              <text x={60} y={22} fontSize="11.5" fill="var(--text-disabled)">
                capped recall ↑ · p95 latency →
              </text>
            </svg>
          </div>
        </Block>

        <div className="mt-14 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1.35fr)_minmax(0,0.65fr)] xl:gap-20">
          <ResponseProfile />

          <div>
            <Block title="Applicability & staleness">
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <KeyRow k="window closed" v="2026-08-09 13:45:00Z" />
                <KeyRow k="profile computed" v="2026-08-09 13:46:12Z" />
                <KeyRow k="evidence age" v="14 min" sub="limit 60 min" />
                <KeyRow k="applicability" v="applicable" tone="verified" mono={false} />
                <KeyRow k="regime at capture" v="L2 · target-075" />
                <KeyRow k="regime now" v="L2 · target-075" />
                <KeyRow k="workload identity" v="wl-search-api-r4" />
              </div>
            </Block>

            <div className="mt-8">
              <Prose>
                Applicability means the profile was captured under the regime that is still current.
                It does not mean the candidate is qualified, admitted, or authorized. Predicted
                improvement at ef 800 is evidence submitted to qualification; activation requires a
                signed grant that has not been presented.
              </Prose>
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
