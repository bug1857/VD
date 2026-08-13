import type { ReactNode } from "react";
import { TopNav } from "@/components/vd/TopNav";

/**
 * Shared page furniture for the VD Control Center prototype.
 * Tonal surfaces, hairline rules, no cards. SIMULATED DATA throughout.
 */

export function PageShell({
  eyebrow,
  title,
  lede,
  blocked,
  facts,
  children,
}: {
  eyebrow: string;
  title: string;
  lede: string;
  blocked?: string;
  facts: { k: string; v: string; tone?: "verified" | "blocked" | "attention" }[];
  children: ReactNode;
}) {
  return (
    <div className="min-h-screen bg-canvas">
      <TopNav />
      <main className="mx-auto max-w-[1680px] px-8 pb-28 xl:px-12">
        <section className="grid grid-cols-1 gap-10 pt-14 lg:grid-cols-[minmax(0,1fr)_300px] lg:gap-20">
          <div>
            <p className="mono text-[11px] tracking-[0.08em] text-ink-4">
              {eyebrow.toUpperCase()}
            </p>
            <h1 className="mt-3 max-w-[26ch] text-[32px] font-medium leading-[1.26] tracking-[-0.028em] text-ink">
              {title}
            </h1>
            <p className="mt-4 max-w-[68ch] text-[17px] leading-[1.6] text-ink-2">
              {lede}
            </p>
            {blocked && (
              <p className="mt-2.5 max-w-[68ch] text-[17px] leading-[1.6] text-blocked">
                {blocked}
              </p>
            )}
          </div>

          <dl className="space-y-3 pt-2 text-[13px]">
            {facts.map((f) => (
              <div
                key={f.k}
                className="flex items-baseline gap-4 border-b border-line pb-2.5"
              >
                <dt className="w-[112px] shrink-0 text-ink-4">{f.k}</dt>
                <dd
                  className={`mono ${
                    f.tone === "verified"
                      ? "text-verified"
                      : f.tone === "blocked"
                        ? "text-blocked"
                        : f.tone === "attention"
                          ? "text-attention"
                          : "text-ink-2"
                  }`}
                >
                  {f.v}
                </dd>
              </div>
            ))}
            <div className="pt-2">
              <span className="mono text-[11px] tracking-[0.06em] text-ink-4">
                SIMULATED DATA
              </span>
            </div>
          </dl>
        </section>

        {children}

        <p className="mt-16 max-w-[86ch] text-[12.5px] leading-[1.7] text-ink-4">
          Prototype. All values are simulated and no connection to Milvus or a
          VD deployment exists. This interface displays backend-established
          state only; it never establishes authority, and unresolved states are
          presented fail-closed.
        </p>
      </main>
    </div>
  );
}

export function SectionHead({
  title,
  note,
}: {
  title: string;
  note?: string | undefined;
}) {
  return (
    <div className="flex items-baseline justify-between gap-8">
      <h2 className="text-[13.5px] font-medium tracking-[-0.01em] text-ink-2">
        {title}
      </h2>
      {note && (
        <p className="text-right text-[12.5px] text-ink-4">{note}</p>
      )}
    </div>
  );
}

export function Block({
  title,
  note,
  className = "",
  children,
}: {
  title: string;
  note?: string | undefined;
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={className}>
      <SectionHead title={title} note={note} />
      <div className="mt-5">{children}</div>
    </section>
  );
}

export function KeyRow({
  k,
  v,
  sub,
  tone,
  mono = true,
}: {
  k: string;
  v: string;
  sub?: string | undefined;
  tone?: "verified" | "blocked" | "attention" | "muted";
  mono?: boolean;
}) {
  return (
    <div className="group flex items-baseline gap-6 border-t border-line py-2.5 transition-colors duration-150 hover:bg-hover/40">
      <span className="w-[190px] shrink-0 text-[13px] text-ink-3">{k}</span>
      <span
        className={[
          mono ? "mono tabular-nums" : "",
          "text-[13px]",
          tone === "verified"
            ? "text-verified"
            : tone === "blocked"
              ? "text-blocked"
              : tone === "attention"
                ? "text-attention"
                : tone === "muted"
                  ? "text-ink-4"
                  : "text-ink-2",
        ].join(" ")}
      >
        {v}
      </span>
      {sub && (
        <span className="ml-auto text-right text-[12.5px] text-ink-4">
          {sub}
        </span>
      )}
    </div>
  );
}

export function Prose({ children }: { children: ReactNode }) {
  return (
    <p className="max-w-[92ch] text-[13px] leading-[1.68] text-ink-3">
      {children}
    </p>
  );
}
