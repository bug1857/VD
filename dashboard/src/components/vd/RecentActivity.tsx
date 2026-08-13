import { activity } from "@/lib/vd-data";

export function RecentActivity() {
  return (
    <section aria-label="Recent activity">
      <div className="flex items-baseline justify-between">
        <h2 className="text-[13.5px] font-medium tracking-[-0.01em] text-ink-2">
          Activity stream
        </h2>
        <span className="flex items-baseline gap-2 text-[12.5px] text-ink-4">
          <span className="verify-pulse relative -mb-px inline-block h-1 w-1 rounded-full bg-verified" />
          append-only · last 90 minutes
        </span>
      </div>

      <ul className="mt-4">
        {activity.map((e, i) => (
          <li
            key={e.time + e.title}
            className="settle-in group relative -mx-3 flex items-baseline gap-6 rounded-xs border-t border-line px-3 py-3 transition-colors duration-150 hover:bg-hover/45"
            style={{ animationDelay: `${i * 45}ms` }}
          >
            <span
              aria-hidden="true"
              className={[
                "absolute left-0 top-1/2 h-3.5 w-px -translate-y-1/2 transition-opacity duration-200",
                i === 0 ? "bg-accent opacity-70" : "bg-transparent opacity-0",
              ].join(" ")}
            />
            <span className="mono w-[52px] shrink-0 tabular-nums text-[12.5px] text-ink-4">
              {e.time}
            </span>
            <span
              className={[
                "text-[13.5px]",
                e.tone === "blocked" ? "text-ink" : "text-ink-2",
              ].join(" ")}
            >
              {e.title}
            </span>
            <span
              className={[
                "ml-auto text-right text-[13px]",
                e.tone === "blocked" ? "text-blocked" : "text-ink-3",
              ].join(" ")}
            >
              {e.detail}
            </span>
          </li>
        ))}
      </ul>
    </section>
  );
}
