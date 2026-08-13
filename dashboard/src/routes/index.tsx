import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { TopNav } from "@/components/vd/TopNav";
import { AuthorityLineage } from "@/components/vd/AuthorityLineage";
import { AuthorityInspector } from "@/components/vd/AuthorityInspector";
import { VectorSearchField } from "@/components/vd/VectorSearchField";
import { ResponseProfile } from "@/components/vd/ResponseProfile";
import { RecentActivity } from "@/components/vd/RecentActivity";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Overview — VD Control Center" },
      {
        name: "description",
        content:
          "Adaptive vector-database control for Milvus: operating state, authority lineage, response evidence and rollback readiness.",
      },
      { property: "og:title", content: "Overview — VD Control Center" },
      {
        property: "og:description",
        content:
          "Adaptive vector-database control for Milvus: operating state, authority lineage, response evidence and rollback readiness.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: Overview,
});

function Overview() {
  const [stage, setStage] = useState<string | null>(null);

  return (
    <div className="min-h-screen bg-canvas">
      <TopNav />

      <main className="mx-auto max-w-[1680px] px-8 pb-28 xl:px-12">
        {/* Operating state */}
        <section className="grid grid-cols-1 gap-10 pt-14 lg:grid-cols-[minmax(0,1fr)_300px] lg:gap-20">
          <div>
            <h1 className="max-w-[22ch] text-[34px] font-medium leading-[1.24] tracking-[-0.028em] text-ink">
              VD is serving ef 400.
            </h1>
            <p className="mt-5 max-w-[62ch] text-[18.5px] leading-[1.58] text-ink-2">
              Workload remains within the current regime. A candidate transition to ef 800 is
              qualified and admitted, but cannot execute.
            </p>
            <p className="mt-2.5 max-w-[62ch] text-[18.5px] leading-[1.58] text-blocked">
              Missing signed activation grant.
            </p>
            <p className="mt-6 max-w-[70ch] text-[13.5px] leading-[1.65] text-ink-3">
              Predictive evidence is applicable — the response profile was refreshed 14 minutes ago
              from 1,200 observations. Prediction does not authorize execution.
            </p>
          </div>

          <dl className="space-y-3 pt-2 text-[13px]">
            <Fact k="regime" v="L2 · target-075" />
            <Fact k="LKG" v="ef 400" />
            <Fact k="candidate" v="ef 800" />
            <Fact k="rollback" v="ready" tone="verified" />
            <Fact k="evidence age" v="14 min" />
            <div className="pt-2">
              <span className="mono text-[11px] tracking-[0.06em] text-ink-4">SIMULATED DATA</span>
            </div>
          </dl>
        </section>

        {/* Authority lineage */}
        <div className="mt-10 border-t border-line-strong pt-10">
          <AuthorityLineage selected={stage} onSelect={setStage} />
        </div>

        {/* Live compute + response evidence */}
        <div className="mt-16 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,0.86fr)_minmax(0,1.14fr)] xl:gap-20">
          <div className="tonal -mx-6 self-start rounded-md px-6 py-7">
            <VectorSearchField />
          </div>
          <div className="pt-1">
            <ResponseProfile />
          </div>
        </div>

        {/* Recent activity */}
        <div className="mt-20 max-w-[1080px]">
          <RecentActivity />
        </div>

        <p className="mt-16 max-w-[86ch] text-[12.5px] leading-[1.7] text-ink-4">
          Prototype. All values are simulated and no connection to Milvus or a VD deployment exists.
          This interface displays backend-established state only; it never establishes authority,
          and unresolved states are presented fail-closed.
        </p>
      </main>

      <AuthorityInspector stageId={stage} onClose={() => setStage(null)} />
    </div>
  );
}

function Fact({ k, v, tone }: { k: string; v: string; tone?: "verified" }) {
  return (
    <div className="flex items-baseline gap-4 border-b border-line pb-2.5">
      <dt className="w-[104px] shrink-0 text-ink-4">{k}</dt>
      <dd className={`mono ${tone === "verified" ? "text-verified" : "text-ink-2"}`}>{v}</dd>
    </div>
  );
}
