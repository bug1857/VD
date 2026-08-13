import { createFileRoute } from "@tanstack/react-router";
import { PageShell, Block, KeyRow, Prose } from "@/components/vd/page";

export const Route = createFileRoute("/canary")({
  head: () => ({
    meta: [
      { title: "Canary Operations — VD Control Center" },
      {
        name: "description",
        content:
          "Candidate partition state, candidate versus last-known-good traffic share, rollback readiness and unmet live-run prerequisites.",
      },
      { property: "og:title", content: "Canary Operations — VD Control Center" },
      {
        property: "og:description",
        content:
          "Candidate routing state, traffic share and unmet prerequisites for a live canary run.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: CanaryPage,
});

const prerequisites = [
  {
    k: "Qualified candidate evidence",
    v: "present",
    state: "met",
    src: "qualification digest b7714c2e…08",
  },
  {
    k: "Admission receipt",
    v: "adm-eval 6c22…f1",
    state: "met",
    src: "admission service",
  },
  {
    k: "Applicable response profile",
    v: "14 min old",
    state: "met",
    src: "profile digest a83e5c17…19",
  },
  {
    k: "Rollback path verified",
    v: "restoration target ef 400",
    state: "met",
    src: "rollback preflight 12:57:36Z",
  },
  {
    k: "Candidate partition provisioned",
    v: "not provisioned",
    state: "unmet",
    src: "router control-plane read",
  },
  {
    k: "Signed activation grant",
    v: "absent — SIGNED_GRANT_REQUIRED",
    state: "blocked",
    src: "authority service 14:00:03Z",
  },
] as const;

function CanaryPage() {
  return (
    <PageShell
      eyebrow="Canary operations"
      title="No candidate traffic is routed."
      lede="The candidate ef 800 transition holds an admission receipt and applicable predictive evidence. Routing has not been reached: no candidate partition is provisioned and no signed activation grant exists."
      blocked="Live canary is unavailable — fail-closed."
      facts={[
        { k: "serving", v: "ef 400 · LKG" },
        { k: "candidate", v: "ef 800 · admitted" },
        { k: "candidate share", v: "0.00 %", tone: "blocked" },
        { k: "partition", v: "not provisioned" },
        { k: "rollback", v: "ready", tone: "verified" },
        { k: "authority", v: "grant absent", tone: "blocked" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <Block
          title="Routing state"
          note="Traffic share by configuration. The candidate lane is a reserved shape, not a live route."
        >
          <div className="tonal -mx-6 rounded-md px-6 py-8">
            <div className="flex items-baseline justify-between">
              <span className="text-[13px] text-ink-2">serving · last-known-good ef 400</span>
              <span className="mono text-[13px] tabular-nums text-ink">100.00 %</span>
            </div>
            <div className="mt-2.5 h-[10px] w-full rounded-[2px] bg-ink-2/45" />

            <div className="mt-8 flex items-baseline justify-between">
              <span className="text-[13px] text-ink-3">
                candidate ef 800 — reserved lane, never routed
              </span>
              <span className="mono text-[13px] tabular-nums text-blocked">0.00 %</span>
            </div>
            <div
              className="mt-2.5 h-[10px] w-full rounded-[2px]"
              style={{
                backgroundImage:
                  "repeating-linear-gradient(135deg, color-mix(in oklab, var(--accent) 26%, transparent) 0 6px, transparent 6px 12px)",
                boxShadow: "inset 0 0 0 1px var(--border-subtle)",
              }}
            />
            <p className="mt-3 text-[12.5px] text-ink-4">
              The hatched lane depicts the shape a canary would take if authorized. It is not a
              route, and no traffic has ever traversed it.
            </p>

            <div className="mt-10 flex items-center gap-4 border-t border-line pt-5">
              <span className="text-[13px] text-ink-3">Rollback / restoration</span>
              <span className="h-px flex-1 bg-line-strong" />
              <span className="text-[13px] text-verified">Independent safety route ready</span>
              <span className="mono text-[12.5px] text-ink-3">restoration target ef 400</span>
            </div>
          </div>
        </Block>

        <div className="mt-12 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)] xl:gap-20">
          <Block
            title="Live-run prerequisites"
            note="Each statement carries its source. No control on this page can start or stop a run."
          >
            <div>
              {prerequisites.map((p) => (
                <div
                  key={p.k}
                  className="group grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)] items-baseline gap-x-8 border-t border-line py-3 transition-colors duration-150 hover:bg-hover/40"
                >
                  <div className="flex items-baseline gap-3">
                    <span
                      className={[
                        "mono text-[12px]",
                        p.state === "met"
                          ? "text-verified"
                          : p.state === "blocked"
                            ? "text-blocked"
                            : "text-ink-4",
                      ].join(" ")}
                    >
                      {p.state === "met" ? "✓" : p.state === "blocked" ? "×" : "—"}
                    </span>
                    <span className="text-[13.5px] text-ink-2">{p.k}</span>
                  </div>
                  <div className="text-right">
                    <div
                      className={[
                        "mono text-[12.5px]",
                        p.state === "blocked" ? "text-blocked" : "text-ink-2",
                      ].join(" ")}
                    >
                      {p.v}
                    </div>
                    <div className="text-[12px] text-ink-4">{p.src}</div>
                  </div>
                </div>
              ))}
            </div>
          </Block>

          <div>
            <Block title="Candidate record">
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <KeyRow k="candidate ef" v="800" />
                <KeyRow k="admission receipt" v="adm-eval 6c22…f1" />
                <KeyRow k="admitted at" v="2026-08-09 13:42:07Z" />
                <KeyRow k="predicted recall" v="0.964" sub="LCB 0.951" />
                <KeyRow k="predicted p95" v="31.7 ms" sub="UCB 34.6 ms" />
                <KeyRow k="routing state" v="not reached" tone="muted" mono={false} />
                <KeyRow k="execution evidence" v="none" tone="muted" mono={false} />
              </div>
            </Block>

            <div className="mt-8">
              <Prose>
                Admission is not a signed grant. Even with a grant, routing would produce execution
                evidence only after traffic is served and recorded; a grant alone would never be
                presented here as a successful run. Rollback remains a higher-priority path and is
                available independent of any canary state.
              </Prose>
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
