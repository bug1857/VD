import { createFileRoute } from "@tanstack/react-router";
import { PageShell, Block, Prose } from "@/components/vd/page";
import { controlProfile } from "@/lib/vd-data";

export const Route = createFileRoute("/control-profile")({
  head: () => ({
    meta: [
      { title: "Control Profile — VD Control Center" },
      {
        name: "description",
        content:
          "Read-only governed control profile: operator-configurable bounds separated from protocol invariants, with revision and digest.",
      },
      { property: "og:title", content: "Control Profile — VD Control Center" },
      {
        property: "og:description",
        content: "Read-only governed control profile with operator bounds and protocol invariants.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: ControlProfilePage,
});

function ControlProfilePage() {
  const operator = controlProfile.filter((c) => c.kind === "operator");
  const invariant = controlProfile.filter((c) => c.kind === "invariant");

  return (
    <PageShell
      eyebrow="Control profile"
      title="prod-conservative · revision r14."
      lede="The active control profile bounds every decision the system may reach. Operator-configurable values are governed and versioned; protocol invariants cannot be relaxed by configuration, by an operator, or by this interface."
      facts={[
        { k: "profile", v: "prod-conservative" },
        { k: "revision", v: "r14" },
        { k: "digest", v: "3ab90f6d…4e" },
        { k: "published", v: "2026-08-04 09:12Z" },
        { k: "editable here", v: "no" },
        { k: "supersedes", v: "r13" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <div className="grid grid-cols-1 gap-14 xl:grid-cols-2 xl:gap-20">
          <Block
            title="Operator-configurable bounds"
            note="Changed only through a governed profile revision, never in this interface."
          >
            <div>
              {operator.map((c) => (
                <ValueRow key={c.key} c={c} />
              ))}
            </div>
          </Block>

          <Block
            title="Protocol invariants"
            note="Fixed by the protocol. Not operator-configurable at any revision."
          >
            <div>
              {invariant.map((c) => (
                <ValueRow key={c.key} c={c} invariant />
              ))}
            </div>
          </Block>
        </div>

        <div className="mt-14 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)] xl:gap-20">
          <Block title="Revision lineage" note="append-only">
            <div>
              {[
                {
                  r: "r14",
                  d: "3ab90f6d…4e",
                  t: "2026-08-04 09:12Z",
                  n: "raised p95 ceiling to 24.0 ms",
                  cur: true,
                },
                {
                  r: "r13",
                  d: "c17e4402…a8",
                  t: "2026-07-21 16:40Z",
                  n: "added ef 1600 to permitted ladder",
                },
                {
                  r: "r12",
                  d: "58fa1cd9…22",
                  t: "2026-07-02 11:05Z",
                  n: "recall floor 0.915 → 0.920",
                },
              ].map((rev) => (
                <div
                  key={rev.r}
                  className="grid grid-cols-[70px_150px_190px_1fr] items-baseline gap-x-6 border-t border-line py-3 transition-colors hover:bg-hover/40"
                >
                  <span
                    className={["mono text-[13px]", rev.cur ? "text-ink" : "text-ink-4"].join(" ")}
                  >
                    {rev.r}
                  </span>
                  <span className="mono text-[12.5px] text-ink-3">{rev.d}</span>
                  <span className="mono text-[12.5px] text-ink-4">{rev.t}</span>
                  <span className="text-[13px] text-ink-3">{rev.n}</span>
                </div>
              ))}
            </div>
          </Block>

          <div>
            <Block title="Governed source" note="read-only rendering">
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <pre className="mono overflow-x-auto text-[12.5px] leading-[1.75] text-ink-3">
                  {`profile: prod-conservative
revision: r14
recall:
  floor: 0.920
latency:
  p95_ceiling_ms: 24.0
ef:
  ladder: [200, 400, 800, 1600]
  serving: 400
qualification:
  epochs: 2          # invariant
  windows: 12        # invariant
authorization:
  mode: signed-grant-required   # invariant
rollback:
  supremacy: enabled            # invariant`}
                </pre>
              </div>
              <p className="mt-3 text-[12.5px] text-ink-4">
                Rendering only. This prototype performs no configuration mutation and exposes no
                editor.
              </p>
            </Block>

            <div className="mt-8">
              <Prose>
                A profile bounds what may be qualified and admitted. It never grants authority: even
                a profile that permits ef 800 leaves activation dependent on a signed grant.
              </Prose>
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}

function ValueRow({
  c,
  invariant,
}: {
  c: { key: string; value: string; note: string };
  invariant?: boolean;
}) {
  return (
    <div className="group grid grid-cols-[minmax(0,1fr)_minmax(0,0.9fr)] items-baseline gap-x-8 border-t border-line py-3 transition-colors duration-150 hover:bg-hover/40">
      <div>
        <div className="mono text-[13px] text-ink-2">{c.key}</div>
        <div className="text-[12.5px] text-ink-4">{c.note}</div>
      </div>
      <div className="text-right">
        <span
          className={["mono text-[13px] tabular-nums", invariant ? "text-ink-3" : "text-ink"].join(
            " ",
          )}
        >
          {c.value}
        </span>
        {invariant && <div className="text-[12px] text-ink-4">protocol invariant</div>}
      </div>
    </div>
  );
}
