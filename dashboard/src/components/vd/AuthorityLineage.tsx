import { stages, type Stage } from "@/lib/vd-data";

export function AuthorityLineage({
  selected,
  onSelect,
}: {
  selected: string | null;
  onSelect: (id: string) => void;
}) {
  const blockedIndex = stages.findIndex((s) => s.state === "blocked");

  return (
    <section aria-label="Authority lineage" className="w-full">
      <div className="flex items-baseline justify-between">
        <h2 className="text-[13.5px] font-medium tracking-[-0.01em] text-ink-2">
          Authority lineage
        </h2>
        <p className="text-[12.5px] text-ink-4">
          Backend-established state. This interface is not an authority source.
        </p>
      </div>

      <div className="mt-5 flex items-stretch">
        {stages.map((stage, i) => (
          <StageCell
            key={stage.id}
            stage={stage}
            index={i}
            blockedIndex={blockedIndex}
            selected={selected === stage.id}
            onSelect={onSelect}
          />
        ))}
      </div>

      <p className="mt-4 text-[13.5px] text-ink-2">
        <span className="text-blocked">Reason</span>{" "}
        <span className="text-ink-2">
          Signed activation grant unavailable — routing and execution remain
          unreached.
        </span>
      </p>

      <div className="mt-6 flex items-center gap-4 border-t border-line pt-4">
        <span className="text-[13px] text-ink-3">Rollback / restoration</span>
        <span className="h-px flex-1 bg-line-strong" />
        <span className="text-[13px] text-verified">
          Independent safety route ready
        </span>
        <span className="mono text-[12.5px] text-ink-3">
          restoration target ef 400
        </span>
      </div>
    </section>
  );
}

function StageCell({
  stage,
  index,
  blockedIndex,
  selected,
  onSelect,
}: {
  stage: Stage;
  index: number;
  blockedIndex: number;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const verifiedFlow = index < blockedIndex;
  const isBlocked = stage.state === "blocked";
  const inactive = stage.state === "inactive";

  const glyph = isBlocked ? "×" : inactive ? "—" : "✓";

  return (
    <button
      type="button"
      onClick={() => onSelect(stage.id)}
      aria-pressed={selected}
      className="group relative flex-1 self-start rounded-xs pr-6 text-left transition-colors duration-150 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-accent-dim"
    >
      {/* rail */}
      <div className="relative mb-3 h-px w-full">
        <span
          className={[
            "absolute inset-0",
            verifiedFlow ? "bg-line-strong" : "bg-line",
          ].join(" ")}
        />
        {verifiedFlow && (
          <span className="absolute inset-0 opacity-70 flow-line" />
        )}
        {isBlocked && (
          <span className="absolute left-0 top-1/2 h-2.5 w-px -translate-y-1/2 bg-blocked" />
        )}
        <span
          className={[
            "absolute left-0 top-1/2 h-[5px] w-[5px] -translate-y-1/2 rounded-full",
            isBlocked
              ? "bg-blocked"
              : inactive
                ? "bg-ink-4/60"
                : "bg-ink-2/70",
          ].join(" ")}
        />
      </div>

      <div className="mb-1 flex items-baseline gap-2">
        <span className="mono text-[11px] tracking-[0.06em] text-ink-4">
          {String(index + 1).padStart(2, "0")}
        </span>
      </div>

      <div className="flex items-baseline gap-1.5">
        <span
          className={[
            "text-[14.5px] tracking-[-0.012em] transition-colors duration-150",
            isBlocked
              ? "font-medium text-ink"
              : inactive
                ? "text-ink-4"
                : "text-ink-2 group-hover:text-ink",
          ].join(" ")}
        >
          {stage.label}
        </span>
        <span
          className={[
            "text-[13px]",
            isBlocked
              ? "text-blocked"
              : inactive
                ? "text-ink-4"
                : "text-verified/80",
          ].join(" ")}
        >
          {glyph}
        </span>
      </div>

      <p
        className={[
          "mt-1.5 pr-3 text-[12.5px] leading-[1.55]",
          isBlocked ? "text-ink-2" : inactive ? "text-ink-4" : "text-ink-3",
        ].join(" ")}
      >
        {stage.summary}
      </p>

      <span
        className={[
          "absolute -bottom-2 left-0 h-px transition-all duration-200",
          selected ? "w-[calc(100%-24px)] bg-accent" : "w-0 bg-transparent",
        ].join(" ")}
      />
    </button>
  );
}
