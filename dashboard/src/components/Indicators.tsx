import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import type {
  ConnectivityState,
  EvidenceStatus,
  Freshness,
  GateState,
  LifecycleStageState,
  OperationalState,
} from "@/lib/types";

type Tone = "neutral" | "info" | "ok" | "warn" | "danger" | "muted";

const TONE_CLASS: Record<Tone, string> = {
  neutral: "border-border/80 bg-surface-2 text-foreground",
  info: "border-telemetry/40 bg-telemetry/10 text-telemetry",
  ok: "border-ok/40 bg-ok/10 text-ok",
  warn: "border-warn/40 bg-warn/10 text-warn",
  danger: "border-danger/40 bg-danger/10 text-danger",
  muted: "border-border/60 bg-surface-2 text-muted-foreground",
};

export function Pill({
  children,
  tone = "neutral",
  className,
  icon,
}: {
  children: ReactNode;
  tone?: Tone | undefined;
  className?: string | undefined;
  icon?: ReactNode | undefined;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 font-mono text-[11px] font-medium uppercase tracking-[0.08em]",
        TONE_CLASS[tone],
        className,
      )}
    >
      {icon}
      {children}
    </span>
  );
}

export const operationalTone = (state: OperationalState | undefined): Tone => {
  switch (state) {
    case "NOMINAL":
      return "ok";
    case "WATCH":
      return "info";
    case "WARNING":
      return "warn";
    case "CRITICAL":
      return "danger";
    default:
      return "muted";
  }
};

export function OperationalPill({ state }: { state?: OperationalState | undefined }) {
  return <Pill tone={operationalTone(state)}>{state ?? "UNKNOWN"}</Pill>;
}

export function ConnectivityPill({ state }: { state?: ConnectivityState | undefined }) {
  const tone: Tone =
    state === "CONNECTED"
      ? "ok"
      : state === "DEGRADED"
        ? "warn"
        : state === "UNAUTHORIZED"
          ? "danger"
          : "danger";
  return (
    <Pill tone={tone}>
      <span
        aria-hidden
        className={cn(
          "h-1.5 w-1.5 rounded-full",
          state === "CONNECTED" ? "animate-pulse bg-ok" : "bg-current",
        )}
      />
      {state ?? "UNKNOWN"}
    </Pill>
  );
}

export function FreshnessPill({ freshness }: { freshness?: Freshness | undefined }) {
  const state = freshness?.state ?? "UNKNOWN";
  const tone: Tone =
    state === "FRESH" ? "ok" : state === "AGING" ? "warn" : state === "STALE" ? "danger" : "muted";
  const age = freshness?.ageSeconds;
  return (
    <Pill tone={tone} className="normal-case">
      {state}
      {age !== undefined && <span className="text-muted-foreground">· {formatAge(age)}</span>}
    </Pill>
  );
}

export function GatePill({ state }: { state: GateState }) {
  const tone: Tone =
    state === "PASS" ? "ok" : state === "WARN" ? "warn" : state === "UNKNOWN" ? "muted" : "danger";
  return <Pill tone={tone}>{state}</Pill>;
}

export function LifecyclePill({ state }: { state: LifecycleStageState }) {
  const tone: Tone =
    state === "COMPLETE"
      ? "ok"
      : state === "ACTIVE"
        ? "info"
        : state === "BLOCKED" || state === "FAILED"
          ? "danger"
          : "muted";
  return <Pill tone={tone}>{state}</Pill>;
}

const EVIDENCE_TONE: Record<EvidenceStatus, Tone> = {
  "FRESHLY VERIFIED": "ok",
  "REPRODUCIBLE BUT NOT EXECUTED": "info",
  "HISTORICAL ONLY": "muted",
  CONTRADICTED: "warn",
  UNVERIFIABLE: "muted",
  BLOCKED: "danger",
};

export function EvidencePill({ status }: { status: EvidenceStatus }) {
  return (
    <Pill tone={EVIDENCE_TONE[status]} className="tracking-[0.04em]">
      {status}
    </Pill>
  );
}

export function formatAge(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3_600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${(seconds / 3_600).toFixed(1)}h ago`;
  return `${(seconds / 86_400).toFixed(1)}d ago`;
}

export function formatTime(iso?: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toISOString().slice(11, 16) + "Z";
}

export function formatDateTime(iso?: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toISOString().slice(0, 16).replace("T", " ") + "Z";
}

export function formatNumber(value?: number | string, unit?: string, digits = 4): string {
  if (value === undefined || value === null) return "—";
  if (typeof value === "string") return unit ? `${value}${unit}` : value;
  const abs = Math.abs(value);
  const formatted =
    abs >= 1000
      ? value.toLocaleString("en-US")
      : abs >= 10
        ? value.toFixed(1)
        : value.toFixed(digits);
  return unit ? `${formatted}${unit === "%" ? "" : " "}${unit}` : formatted;
}

export function formatDelta(value?: number, unit?: string): string {
  if (value === undefined) return "—";
  const sign = value > 0 ? "+" : "";
  const digits = Math.abs(value) >= 1 ? 2 : 4;
  return `${sign}${formatNumber(value, unit, digits)}`;
}
