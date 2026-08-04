import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { OperationalPill } from "./Indicators";
import type { OperationalState } from "@/lib/types";

export function MetricCard({
  label,
  value,
  unit,
  state,
  detail,
  threshold,
  className,
}: {
  label: string;
  value?: ReactNode | undefined;
  unit?: string | undefined;
  state?: OperationalState | undefined;
  detail?: string | undefined;
  threshold?: number | undefined;
  className?: string | undefined;
}) {
  return (
    <div className={cn("panel px-4 py-3.5", className)}>
      <div className="flex items-start justify-between gap-2">
        <span className="label-mono">{label}</span>
        {state && <OperationalPill state={state} />}
      </div>
      <div className="mt-2.5 flex items-baseline gap-1.5">
        <span className="metric-value">{value ?? "—"}</span>
        {unit && <span className="font-mono text-xs text-muted-foreground">{unit}</span>}
      </div>
      {(detail || threshold !== undefined) && (
        <p className="mt-1.5 text-[11px] text-muted-foreground">
          {detail}
          {detail && threshold !== undefined ? " · " : ""}
          {threshold !== undefined ? `threshold ${threshold}` : ""}
        </p>
      )}
    </div>
  );
}
