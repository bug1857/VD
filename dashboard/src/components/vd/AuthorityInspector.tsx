import { useEffect, useState } from "react";
import { stages } from "@/lib/vd-data";

export function AuthorityInspector({
  stageId,
  onClose,
}: {
  stageId: string | null;
  onClose: () => void;
}) {
  const stage = stages.find((s) => s.id === stageId) ?? null;

  useEffect(() => {
    if (!stage) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [stage, onClose]);

  if (!stage) return null;

  const tone =
    stage.state === "blocked"
      ? "text-blocked"
      : stage.state === "inactive"
        ? "text-ink-4"
        : "text-verified";
  const stateWord =
    stage.state === "blocked"
      ? "Blocked"
      : stage.state === "inactive"
        ? "Not reached"
        : "Verified";

  return (
    <aside
      role="dialog"
      aria-label={`${stage.label} authority detail`}
      className="fixed right-0 top-0 z-40 flex h-screen w-[420px] flex-col bg-raised shadow-[-24px_0_60px_-30px_rgba(0,0,0,0.8)] panel-in"
    >
      <div className="absolute inset-y-0 left-0 w-px bg-line-strong" />

      <div className="flex items-start justify-between px-7 pb-5 pt-6">
        <div>
          <p className="mono text-[11px] tracking-[0.08em] text-ink-4">
            EVIDENCE RECORD
          </p>
          <h3 className="mt-1.5 text-[16.5px] font-medium tracking-[-0.015em] text-ink">
            {stage.label}
          </h3>
          <p className={`mt-1 text-[13.5px] ${tone}`}>{stateWord}</p>
        </div>
        <button
          onClick={onClose}
          aria-label="Close inspector"
          className="-mr-2 rounded-xs px-2 py-1 text-[13px] text-ink-4 transition-colors hover:bg-hover hover:text-ink-2"
        >
          Close
        </button>
      </div>

      <div className="flex-1 overflow-y-auto px-7 pb-10">
        <p className="text-[13.5px] leading-[1.65] text-ink-2">
          {stage.detail}
        </p>

        {stage.dependency && (
          <Section label="Dependency">
            <p className="text-[13.5px] text-ink-2">{stage.dependency}</p>
          </Section>
        )}

        {stage.evidence && (
          <Section label="Evidence">
            <dl className="space-y-2.5">
              {stage.evidence.map((e) => (
                <EvidenceRow key={e.label} label={e.label} value={e.value} />
              ))}
            </dl>
          </Section>
        )}

        {stage.reasonCode && (
          <Section label="Blocking reason">
            <div className="-mx-2 rounded-xs bg-hover/50 px-3 py-2.5">
              <span className="mono text-[13px] tracking-[0.02em] text-blocked">
                {stage.reasonCode}
              </span>
              <p className="mt-1.5 text-[12.5px] leading-[1.55] text-ink-3">
                Fail-closed. Downstream stages remain unreached until a signed
                grant is presented.
              </p>
            </div>
          </Section>
        )}

        <p className="mt-10 border-t border-line pt-4 text-[12.5px] leading-[1.6] text-ink-4">
          SIMULATED DATA. This panel reflects prototype state only and performs
          no authority evaluation.
        </p>
      </div>
    </aside>
  );
}

function Section({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="mt-7 border-t border-line pt-4">
      <p className="mb-2.5 mono text-[11px] tracking-[0.07em] text-ink-4">
        {label.toUpperCase()}
      </p>
      {children}
    </div>
  );
}

function EvidenceRow({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="group -mx-2 flex items-baseline gap-3 rounded-xs px-2 py-1 transition-colors hover:bg-hover/70">
      <dt className="w-[132px] shrink-0 text-[12.5px] text-ink-3">{label}</dt>
      <dd className="mono flex-1 break-all tabular-nums text-[12.5px] text-ink-2">
        {value}
      </dd>
      <button
        onClick={() => {
          navigator.clipboard?.writeText(value);
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        }}
        className="text-[12px] text-ink-4 opacity-0 transition-opacity hover:text-ink-2 group-hover:opacity-100 focus:opacity-100"
      >
        {copied ? "copied" : "copy"}
      </button>
    </div>
  );
}
