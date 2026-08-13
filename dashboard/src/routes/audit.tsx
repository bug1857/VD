import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { PageShell, Block, KeyRow, Prose } from "@/components/vd/page";
import { evidenceLedger } from "@/lib/vd-data";

export const Route = createFileRoute("/audit")({
  head: () => ({
    meta: [
      { title: "Audit & Evidence — VD Control Center" },
      {
        name: "description",
        content:
          "Append-only evidence ledger with digest lineage, receipts, provenance and per-entry verification status.",
      },
      { property: "og:title", content: "Audit & Evidence — VD Control Center" },
      {
        property: "og:description",
        content:
          "Append-only evidence ledger with digest lineage, receipts and verification status.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: AuditPage,
});

function AuditPage() {
  const [sel, setSel] = useState<number>(evidenceLedger[0]!.seq);
  const [copied, setCopied] = useState<string | null>(null);
  const e = evidenceLedger.find((x) => x.seq === sel)!;

  const copy = (v: string) => {
    navigator.clipboard?.writeText(v);
    setCopied(v);
    setTimeout(() => setCopied(null), 1200);
  };

  return (
    <PageShell
      eyebrow="Audit & evidence"
      title="Evidence chain intact to sequence 4417."
      lede="Every observation, prediction, qualification, admission and refusal is appended as an immutable entry. Entries are never amended; a correction is a later entry that supersedes an earlier one."
      facts={[
        { k: "head sequence", v: "4417" },
        { k: "chain", v: "intact", tone: "verified" },
        { k: "entries today", v: "38" },
        { k: "refusals", v: "2", tone: "blocked" },
        { k: "ledger digest", v: "9d41ab07…c2" },
        { k: "self-verified", v: "14:02:04Z" },
      ]}
    >
      <div className="mt-14 border-t border-line-strong pt-10">
        <div className="grid grid-cols-1 gap-14 xl:grid-cols-[minmax(0,1.35fr)_minmax(0,0.65fr)] xl:gap-20">
          <Block
            title="Evidence timeline"
            note="Append-only · newest first. Hashes are truncated for reading; copy yields the stored value."
          >
            <div className="relative">
              <span
                aria-hidden="true"
                className="absolute bottom-2 left-[86px] top-2 w-px bg-line"
              />
              {evidenceLedger.map((entry, i) => {
                const active = entry.seq === sel;
                return (
                  <button
                    key={entry.seq}
                    type="button"
                    onClick={() => setSel(entry.seq)}
                    className={[
                      "settle-in relative grid w-full grid-cols-[86px_minmax(0,1fr)_150px] items-baseline gap-x-8 border-t border-line py-3.5 text-left transition-colors duration-150",
                      active ? "bg-hover/50" : "hover:bg-hover/35",
                    ].join(" ")}
                    style={{ animationDelay: `${i * 40}ms` }}
                  >
                    <span className="mono text-[12.5px] tabular-nums text-ink-4">
                      {entry.seq}
                    </span>
                    <span className="relative pl-6">
                      <span
                        aria-hidden="true"
                        className={[
                          "absolute left-[-1px] top-[7px] h-[5px] w-[5px] -translate-x-1/2 rounded-full",
                          entry.verification === "refused"
                            ? "bg-blocked"
                            : active
                              ? "bg-ink-2"
                              : "bg-ink-4/70",
                        ].join(" ")}
                      />
                      <span className="mono text-[13px] text-ink-2">
                        {entry.kind}
                      </span>
                      <span
                        className={[
                          "ml-3 text-[13px]",
                          entry.tone === "blocked"
                            ? "text-blocked"
                            : "text-ink-3",
                        ].join(" ")}
                      >
                        {entry.subject}
                      </span>
                      <span className="mt-1 block mono text-[11.5px] text-ink-4">
                        {entry.time}
                      </span>
                    </span>
                    <span className="text-right">
                      <span className="mono block text-[12px] text-ink-3">
                        {entry.digest}
                      </span>
                      <span
                        className={[
                          "text-[12px]",
                          entry.verification === "verified"
                            ? "text-verified"
                            : entry.verification === "refused"
                              ? "text-blocked"
                              : "text-ink-4",
                        ].join(" ")}
                      >
                        {entry.verification}
                      </span>
                    </span>
                  </button>
                );
              })}
            </div>
          </Block>

          <div>
            <Block title="Entry provenance" note={`seq ${e.seq}`}>
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <KeyRow k="kind" v={e.kind} />
                <KeyRow k="subject" v={e.subject} />
                <KeyRow k="recorded at" v={e.time} />
                <KeyRow
                  k="verification"
                  v={e.verification}
                  tone={
                    e.verification === "verified"
                      ? "verified"
                      : e.verification === "refused"
                        ? "blocked"
                        : "muted"
                  }
                  mono={false}
                />
                <div className="group flex items-baseline gap-6 border-t border-line py-2.5 transition-colors hover:bg-hover/40">
                  <span className="w-[190px] shrink-0 text-[13px] text-ink-3">
                    entry digest
                  </span>
                  <span className="mono text-[13px] text-ink-2">
                    {e.digest}
                  </span>
                  <button
                    onClick={() => copy(e.digest)}
                    className="ml-auto text-[12px] text-ink-4 opacity-0 transition-opacity hover:text-ink-2 group-hover:opacity-100 focus:opacity-100"
                  >
                    {copied === e.digest ? "copied" : "copy"}
                  </button>
                </div>
                <KeyRow k="predecessor" v={`seq ${e.seq - 1}`} />
                <KeyRow k="signature" v="ledger key kid-7f31" />
              </div>
            </Block>

            <Block className="mt-10" title="Digest lineage">
              <div className="tonal -mx-5 rounded-md px-5 py-4">
                <KeyRow k="observation window" v="77b0d5c8…31" />
                <KeyRow k="response profile" v="a83e5c17…19" />
                <KeyRow k="qualification" v="b7714c2e…08" />
                <KeyRow k="admission receipt" v="6c22e930…f1" />
                <KeyRow
                  k="activation grant"
                  v="absent"
                  tone="blocked"
                  mono={false}
                />
                <KeyRow k="execution evidence" v="none" tone="muted" mono={false} />
              </div>
            </Block>

            <div className="mt-8">
              <Prose>
                Verification status describes the ledger entry, not the outcome
                it records. A verified refusal is an authentic record that
                activation was denied. Absence of an execution record is itself
                evidence: nothing was executed.
              </Prose>
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
