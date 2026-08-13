import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { PageShell, Block, KeyRow, Prose } from "@/components/vd/page";
import { qualWindows } from "@/lib/vd-data";

export const Route = createFileRoute("/lkg")({
  head: () => ({
    meta: [
      { title: "LKG Qualification — VD Control Center" },
      {
        name: "description",
        content:
          "Two-epoch, twelve-window qualification evidence for the last-known-good HNSW ef configuration, with immutable qualification lineage.",
      },
      { property: "og:title", content: "LKG Qualification — VD Control Center" },
      {
        property: "og:description",
        content:
          "Two epochs, twelve windows of qualification evidence for the last-known-good ef configuration.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: LkgPage,
});

function LkgPage() {
  const [sel, setSel] = useState<string>(qualWindows[8]!.id);
  const w = qualWindows.find((q) => q.id === sel)!;
  const floor = 0.92;
  const ceiling = 24;

  return (
    <PageShell
      eyebrow="LKG qualification"
      title="ef 400 is qualified as last-known-good."
      lede="Qualification requires two epochs of six observation windows each. All twelve windows cleared the recall floor and the p95 ceiling of control profile prod-conservative. Qualification is a property of evidence — it does not route traffic."
      facts={[
        { k: "qualified ef", v: "400", tone: "verified" },
        { k: "structure", v: "2 epochs · 12 windows" },
        { k: "recall floor", v: "0.920" },
        { k: "p95 ceiling", v: "24.0 ms" },
        { k: "qualified at", v: "13:41:58Z" },
        { k: "lineage", v: "immutable" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <Block
          title="Qualification structure"
          note="Each mark is one observation window. Height is capped recall above the floor."
        >
          <div className="tonal -mx-6 rounded-md px-6 py-7">
            <div className="grid grid-cols-2 gap-x-16">
              {([1, 2] as const).map((epoch) => (
                <div key={epoch}>
                  <div className="flex items-baseline justify-between border-b border-line pb-2">
                    <span className="mono text-[11px] tracking-[0.08em] text-ink-4">
                      EPOCH {String(epoch).padStart(2, "0")}
                    </span>
                    <span className="text-[12.5px] text-ink-4">6 windows · all cleared</span>
                  </div>
                  <div className="mt-5 flex items-end gap-3">
                    {qualWindows
                      .filter((q) => q.epoch === epoch)
                      .map((q) => {
                        const h = 14 + ((q.recall - floor) / 0.03) * 92;
                        const active = q.id === sel;
                        return (
                          <button
                            key={q.id}
                            type="button"
                            onMouseEnter={() => setSel(q.id)}
                            onClick={() => setSel(q.id)}
                            className="group flex flex-1 flex-col items-stretch text-left"
                          >
                            <span
                              className={[
                                "w-[22px] rounded-[1px] transition-all duration-200",
                                active
                                  ? "bg-ink-2"
                                  : q.verdict === "pass (marginal)"
                                    ? "bg-attention/50 group-hover:bg-attention/70"
                                    : "bg-ink-3/40 group-hover:bg-ink-3/65",
                              ].join(" ")}
                              style={{ height: `${h}px` }}
                            />

                            <span className="mt-2 border-t border-line pt-1.5 mono text-[11px] text-ink-4">
                              w{q.index}
                            </span>
                            <span
                              className={[
                                "mono text-[11.5px] tabular-nums",
                                active ? "text-ink-2" : "text-ink-4",
                              ].join(" ")}
                            >
                              {q.recall.toFixed(3)}
                            </span>
                          </button>
                        );
                      })}
                  </div>
                </div>
              ))}
            </div>
            <p className="mt-6 border-t border-line pt-3 text-[12.5px] text-ink-4">
              Baseline of each mark is the recall floor 0.920. Amber marks cleared the floor by a
              margin narrower than 0.005.
            </p>
          </div>
        </Block>

        <div className="mt-12 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1.25fr)_minmax(0,0.75fr)] xl:gap-20">
          <Block title="Window evidence" note="12 windows · epoch order">
            <div>
              <div className="grid grid-cols-[110px_100px_100px_120px_1fr] gap-x-6 pb-2 text-[12px] text-ink-4">
                <span>window</span>
                <span className="text-right">recall</span>
                <span className="text-right">p95</span>
                <span className="text-right">observations</span>
                <span className="text-right">verdict</span>
              </div>
              {qualWindows.map((q) => (
                <button
                  key={q.id}
                  type="button"
                  onMouseEnter={() => setSel(q.id)}
                  onClick={() => setSel(q.id)}
                  className={[
                    "grid w-full grid-cols-[110px_100px_100px_120px_1fr] items-baseline gap-x-6 border-t border-line py-2.5 text-left transition-colors duration-150",
                    q.id === sel ? "bg-hover/50" : "hover:bg-hover/35",
                  ].join(" ")}
                >
                  <span className="mono text-[12.5px] text-ink-3">
                    e{q.epoch} · w{q.index}
                  </span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink">
                    {q.recall.toFixed(3)}
                  </span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink-2">
                    {q.p95.toFixed(1)} ms
                  </span>
                  <span className="mono text-right text-[12.5px] tabular-nums text-ink-3">
                    {q.observations}
                  </span>
                  <span
                    className={[
                      "text-right text-[12.5px]",
                      q.verdict === "pass (marginal)" ? "text-attention" : "text-ink-3",
                    ].join(" ")}
                  >
                    {q.verdict}
                  </span>
                </button>
              ))}
            </div>
          </Block>

          <div>
            <Block title="Selected window" note={w.id}>
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <KeyRow k="epoch · window" v={`${w.epoch} · ${w.index}`} />
                <KeyRow
                  k="capped recall"
                  v={w.recall.toFixed(3)}
                  sub={`floor ${floor.toFixed(3)}`}
                />
                <KeyRow
                  k="p95 latency"
                  v={`${w.p95.toFixed(1)} ms`}
                  sub={`ceiling ${ceiling.toFixed(1)} ms`}
                />
                <KeyRow k="observations" v={String(w.observations)} />
                <KeyRow
                  k="verdict"
                  v={w.verdict}
                  tone={w.verdict === "pass (marginal)" ? "attention" : "muted"}
                  mono={false}
                />
              </div>
            </Block>

            <Block className="mt-10" title="Qualification lineage" note="append-only">
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <KeyRow k="qualified ef" v="400" tone="verified" />
                <KeyRow k="control profile" v="prod-conservative · r14" />
                <KeyRow k="qualification digest" v="b7714c2e…08" />
                <KeyRow k="supersedes" v="q-2026-08-08 · ef 400" />
                <KeyRow k="qualified at" v="2026-08-09 13:41:58Z" />
                <KeyRow k="mutability" v="immutable record" mono={false} />
              </div>
            </Block>

            <div className="mt-8">
              <Prose>
                A qualified configuration is eligible for admission. It is not routed by
                qualification alone, and re-qualification of the last-known-good does not extend
                authority to any candidate.
              </Prose>
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
