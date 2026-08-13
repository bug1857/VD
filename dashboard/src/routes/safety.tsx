import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { PageShell, Block, KeyRow, Prose } from "@/components/vd/page";
import { AuthorityLineage } from "@/components/vd/AuthorityLineage";
import { AuthorityInspector } from "@/components/vd/AuthorityInspector";

export const Route = createFileRoute("/safety")({
  head: () => ({
    meta: [
      { title: "Safety & Authority — VD Control Center" },
      {
        name: "description",
        content:
          "Authority chain, signed grant state, admission scope, route authority, rollback supremacy and the exact fail-closed reason.",
      },
      { property: "og:title", content: "Safety & Authority — VD Control Center" },
      {
        property: "og:description",
        content:
          "Authority chain, signed grant state, rollback supremacy and the exact fail-closed reason.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: SafetyPage,
});

const authorities = [
  {
    k: "Signed activation grant",
    v: "absent",
    state: "blocked",
    src: "authority service · evaluated 2026-08-09 14:00:03Z",
    detail:
      "No grant document was presented for the candidate. Without a verified signature over the admission receipt and profile digest, activation is refused.",
  },
  {
    k: "Admission receipt",
    v: "adm-eval 6c22…f1",
    state: "met",
    src: "admission service · 13:42:07Z",
    detail:
      "Admission scopes a candidate to evaluation. It confers no authority to route traffic and expires independently of qualification.",
  },
  {
    k: "Route authority",
    v: "not granted",
    state: "unmet",
    src: "router control-plane · derived from grant",
    detail:
      "Route authority is derived from a signed grant. With no grant, the router holds the last-known-good binding and refuses candidate bindings.",
  },
  {
    k: "Rollback supremacy",
    v: "enabled",
    state: "met",
    src: "control profile invariant · prod-conservative r14",
    detail:
      "Rollback outranks every activation path and does not require an activation grant. Restoration to ef 400 is verified and available now.",
  },
  {
    k: "Fail-closed posture",
    v: "engaged",
    state: "met",
    src: "protocol invariant",
    detail:
      "Any unresolved authority evaluation resolves to refusal. Downstream stages remain unreached rather than being presented as pending success.",
  },
] as const;

function SafetyPage() {
  const [stage, setStage] = useState<string | null>(null);
  const [open, setOpen] = useState<string>(authorities[0]!.k);

  return (
    <>
      <PageShell
        eyebrow="Safety & authority"
        title="Activation is refused. The chain stops at authorization."
        lede="Observation, prediction, qualification and admission all completed and hold evidence. The authority service presented no signed activation grant, so routing and execution were never reached."
        blocked="SIGNED_GRANT_REQUIRED — fail-closed."
        facts={[
          { k: "chain state", v: "blocked at 05", tone: "blocked" },
          { k: "reason code", v: "SIGNED_GRANT_REQUIRED", tone: "blocked" },
          { k: "serving", v: "ef 400 · LKG" },
          { k: "rollback", v: "supreme · ready", tone: "verified" },
          { k: "frontend authority", v: "none" },
          { k: "evaluated", v: "14:00:03Z" },
        ]}
      >
        <div className="mt-14 border-t border-line-strong pt-10">
          <AuthorityLineage selected={stage} onSelect={setStage} />
        </div>

        <div className="mt-16 grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)] xl:gap-20">
          <Block
            title="Authority instruments"
            note="Each instrument states its own source. Select one to read its scope."
          >
            <div>
              {authorities.map((a) => {
                const isOpen = open === a.k;
                return (
                  <div key={a.k} className="border-t border-line">
                    <button
                      type="button"
                      onClick={() => setOpen(a.k)}
                      aria-expanded={isOpen}
                      className={[
                        "grid w-full grid-cols-[minmax(0,1fr)_220px] items-baseline gap-x-8 py-3 text-left transition-colors duration-150",
                        isOpen ? "bg-hover/45" : "hover:bg-hover/30",
                      ].join(" ")}
                    >
                      <span className="flex items-baseline gap-3">
                        <span
                          className={[
                            "mono text-[12px]",
                            a.state === "met"
                              ? "text-verified"
                              : a.state === "blocked"
                                ? "text-blocked"
                                : "text-ink-4",
                          ].join(" ")}
                        >
                          {a.state === "met" ? "✓" : a.state === "blocked" ? "×" : "—"}
                        </span>
                        <span className="text-[14px] text-ink-2">{a.k}</span>
                      </span>
                      <span
                        className={[
                          "mono text-right text-[12.5px]",
                          a.state === "blocked" ? "text-blocked" : "text-ink-2",
                        ].join(" ")}
                      >
                        {a.v}
                      </span>
                    </button>
                    {isOpen && (
                      <div className="settle-in pb-4 pl-7 pr-2">
                        <p className="max-w-[76ch] text-[13px] leading-[1.65] text-ink-3">
                          {a.detail}
                        </p>
                        <p className="mt-2 mono text-[12px] text-ink-4">source · {a.src}</p>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </Block>

          <div>
            <Block title="Refusal record">
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <div className="-mx-1 rounded-xs bg-hover/50 px-3 py-2.5">
                  <span className="mono text-[13px] tracking-[0.02em] text-blocked">
                    SIGNED_GRANT_REQUIRED
                  </span>
                  <p className="mt-1.5 text-[12.5px] leading-[1.55] text-ink-3">
                    Activation refused at stage 05. Routing and execution remain unreached; serving
                    traffic continues on the last-known-good configuration.
                  </p>
                </div>
                <div className="mt-3">
                  <KeyRow k="candidate" v="ef 800" />
                  <KeyRow k="admission receipt" v="adm-eval 6c22…f1" />
                  <KeyRow k="profile digest" v="a83e5c17…19" />
                  <KeyRow k="workload identity" v="wl-search-api-r4" />
                  <KeyRow k="evaluated at" v="2026-08-09 14:00:03Z" />
                  <KeyRow k="refusals today" v="2" />
                </div>
              </div>
            </Block>

            <div className="mt-8">
              <Prose>
                Prediction is not authorization. Qualification is not routing. Admission is not a
                signed grant. A signed grant would not be execution evidence. This interface reports
                backend-established authority state and never establishes it.
              </Prose>
            </div>
          </div>
        </div>
      </PageShell>

      <AuthorityInspector stageId={stage} onClose={() => setStage(null)} />
    </>
  );
}
