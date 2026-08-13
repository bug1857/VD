import { createFileRoute } from "@tanstack/react-router";
import { PageShell, Block, Prose } from "@/components/vd/page";
import { healthItems } from "@/lib/vd-data";

export const Route = createFileRoute("/health")({
  head: () => ({
    meta: [
      { title: "Health — VD Control Center" },
      {
        name: "description",
        content:
          "Milvus, environment, worker, ledger and evidence health with the exact source and check time behind every statement.",
      },
      { property: "og:title", content: "Health — VD Control Center" },
      {
        property: "og:description",
        content: "Component health with the exact source and check time behind every statement.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: HealthPage,
});

const tone = {
  healthy: "text-verified",
  stale: "text-attention",
  degraded: "text-blocked",
  unknown: "text-ink-4",
} as const;

const glyph = {
  healthy: "✓",
  stale: "◐",
  degraded: "×",
  unknown: "—",
} as const;

function HealthPage() {
  const degraded = healthItems.filter((h) => h.state === "degraded");
  const stale = healthItems.filter((h) => h.state === "stale");

  return (
    <PageShell
      eyebrow="Health"
      title="Serving path healthy. Two dependencies are not."
      lede="Health is reported per component with the source that produced each statement. No aggregate verdict is shown: a green summary would hide that the authority service is refusing and the profile worker is stale."
      facts={[
        { k: "serving path", v: "healthy", tone: "verified" },
        { k: "degraded", v: String(degraded.length), tone: "blocked" },
        { k: "stale", v: String(stale.length), tone: "attention" },
        { k: "unknown", v: "1" },
        { k: "rollback", v: "ready", tone: "verified" },
        { k: "last sweep", v: "14:02:11Z" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <Block
          title="Component health"
          note="Every statement carries its producing source and check time."
        >
          <div>
            <div className="grid grid-cols-[minmax(0,0.9fr)_minmax(0,1.3fr)_minmax(0,0.9fr)_120px] gap-x-8 pb-2 text-[12px] text-ink-4">
              <span>component</span>
              <span>statement</span>
              <span>source</span>
              <span className="text-right">checked</span>
            </div>
            {healthItems.map((h) => (
              <div
                key={h.component}
                className="grid grid-cols-[minmax(0,0.9fr)_minmax(0,1.3fr)_minmax(0,0.9fr)_120px] items-baseline gap-x-8 border-t border-line py-3.5 transition-colors duration-150 hover:bg-hover/40"
              >
                <span className="flex items-baseline gap-3">
                  <span className={`mono text-[12px] ${tone[h.state]}`}>{glyph[h.state]}</span>
                  <span className="text-[13.5px] text-ink-2">{h.component}</span>
                </span>
                <span
                  className={[
                    "text-[13px]",
                    h.state === "degraded"
                      ? "text-blocked"
                      : h.state === "stale"
                        ? "text-attention"
                        : "text-ink-3",
                  ].join(" ")}
                >
                  {h.statement}
                </span>
                <span className="mono text-[12.5px] text-ink-4">{h.source}</span>
                <span className="mono text-right text-[12.5px] tabular-nums text-ink-4">
                  {h.checked}
                </span>
              </div>
            ))}
          </div>
        </Block>

        <div className="mt-14 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)] xl:gap-20">
          <Block title="Consequences" note="What each unhealthy dependency prevents">
            <div className="tonal -mx-5 rounded-md px-5 py-4">
              <Consequence
                head="Authority service — degraded"
                body="No signed activation grant can be obtained, so the candidate transition cannot be authorized. Serving continues on the last-known-good configuration."
                t="blocked"
              />
              <Consequence
                head="Response profile worker — stale"
                body="Predictive evidence ages toward its 60-minute applicability limit. At the limit, the candidate becomes inapplicable and re-observation is required."
                t="attention"
              />
              <Consequence
                head="Canary router — unknown"
                body="No candidate partition is provisioned, so a live canary could not be started even if a grant were presented."
              />
            </div>
          </Block>

          <div>
            <Prose>
              Health statements describe dependencies, not authority. A fully healthy environment
              would still leave activation refused while the signed grant is absent, and a degraded
              environment never relaxes the fail-closed posture. Rollback is verified independently
              of every component listed here.
            </Prose>
          </div>
        </div>
      </div>
    </PageShell>
  );
}

function Consequence({
  head,
  body,
  t,
}: {
  head: string;
  body: string;
  t?: "blocked" | "attention";
}) {
  return (
    <div className="border-t border-line py-3 first:border-t-0 first:pt-0">
      <p
        className={[
          "text-[13px]",
          t === "blocked" ? "text-blocked" : t === "attention" ? "text-attention" : "text-ink-3",
        ].join(" ")}
      >
        {head}
      </p>
      <p className="mt-1 max-w-[62ch] text-[13px] leading-[1.62] text-ink-3">{body}</p>
    </div>
  );
}
