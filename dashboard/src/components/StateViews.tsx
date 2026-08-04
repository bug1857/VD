import type { ReactNode } from "react";
import {
  AlertTriangle,
  DatabaseZap,
  Inbox,
  Lock,
  PlugZap,
  RefreshCw,
  ShieldAlert,
} from "lucide-react";
import type { ApiEnvelope, ApiError } from "@/lib/types";
import type { ResourceState } from "@/app/scenario";
import { cn } from "@/lib/utils";
import { FreshnessPill } from "./Indicators";

export function Skeleton({ className }: { className?: string | undefined }) {
  return <div className={cn("animate-pulse rounded-md bg-surface-3", className)} aria-hidden />;
}

export function LoadingBlock({
  label = "Loading telemetry",
  rows = 4,
}: {
  label?: string;
  rows?: number;
}) {
  return (
    <div role="status" aria-live="polite" className="space-y-3">
      <p className="flex items-center gap-2 text-xs text-muted-foreground">
        <RefreshCw className="h-3.5 w-3.5 animate-spin text-telemetry" aria-hidden />
        {label}…
      </p>
      {Array.from({ length: rows }).map((_, i) => (
        <Skeleton key={i} className={cn("h-4", i === 0 ? "w-2/3" : i % 2 ? "w-full" : "w-5/6")} />
      ))}
    </div>
  );
}

function iconFor(code: ApiError["code"]) {
  switch (code) {
    case "BACKEND_NOT_CONNECTED":
      return PlugZap;
    case "UNAUTHORIZED":
      return Lock;
    case "BLOCKED_BY_SAFETY_GATE":
      return ShieldAlert;
    case "NO_DATA":
      return Inbox;
    case "STALE_DATA":
      return DatabaseZap;
    default:
      return AlertTriangle;
  }
}

export function ErrorBlock({
  error,
  onRetry,
  compact,
}: {
  error?: ApiError | undefined;
  onRetry?: (() => void) | undefined;
  compact?: boolean | undefined;
}) {
  const code = error?.code ?? "INTERNAL_ERROR";
  const Icon = iconFor(code);
  const tone =
    code === "BLOCKED_BY_SAFETY_GATE" || code === "UNAUTHORIZED"
      ? "text-danger border-danger/40 bg-danger/5"
      : code === "BACKEND_NOT_CONNECTED"
        ? "text-warn border-warn/40 bg-warn/5"
        : "text-danger border-danger/40 bg-danger/5";

  return (
    <div role="alert" className={cn("rounded-lg border px-4 py-4", tone, compact && "px-3 py-3")}>
      <div className="flex items-start gap-3">
        <Icon className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
        <div className="min-w-0 flex-1">
          <p className="font-mono text-[11px] uppercase tracking-[0.12em]">
            {code.replace(/_/g, " ")}
          </p>
          <p className="mt-1 text-sm font-medium text-foreground">
            {error?.message ?? "Read failed"}
          </p>
          {error?.hint && <p className="mt-1 text-xs text-muted-foreground">{error.hint}</p>}
          {onRetry && error?.retryable && (
            <button type="button" onClick={onRetry} className="btn-ghost mt-3">
              <RefreshCw className="h-3.5 w-3.5" aria-hidden /> Retry read
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

export function EmptyBlock({
  title = "No observations",
  detail = "The selected window contains no data. Nothing is inferred or back-filled.",
}: {
  title?: string | undefined;
  detail?: string | undefined;
}) {
  return (
    <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center">
      <Inbox className="mx-auto h-5 w-5 text-muted-foreground" aria-hidden />
      <p className="mt-3 text-sm font-medium text-foreground">{title}</p>
      <p className="mx-auto mt-1 max-w-sm text-xs text-muted-foreground">{detail}</p>
    </div>
  );
}

export function StaleBanner({ envelope }: { envelope: ApiEnvelope<unknown> }) {
  if (envelope.freshness.state !== "STALE" && envelope.freshness.state !== "UNKNOWN") return null;
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-warn/40 bg-warn/5 px-4 py-2.5">
      <FreshnessPill freshness={envelope.freshness} />
      <p className="text-xs text-warn">
        {envelope.notes?.[0] ??
          "These values are outside their freshness window and may not reflect current behaviour."}
      </p>
    </div>
  );
}

/** Renders exactly one explicit state for a resource. */
export function Resource<T>({
  state,
  onRetry,
  loadingLabel,
  empty,
  children,
}: {
  state: ResourceState<T>;
  onRetry?: (() => void) | undefined;
  loadingLabel?: string | undefined;
  empty?: { title?: string | undefined; detail?: string | undefined } | undefined;
  children: (data: T, envelope: ApiEnvelope<T>) => ReactNode;
}) {
  if (state.kind === "loading") return <LoadingBlock label={loadingLabel ?? "Loading telemetry"} />;
  if (state.kind === "error") return <ErrorBlock error={state.envelope.error} onRetry={onRetry} />;
  if (state.kind === "empty") return <EmptyBlock {...(empty ?? {})} />;
  return (
    <div className="space-y-4">
      <StaleBanner envelope={state.envelope} />
      {children(state.data, state.envelope)}
    </div>
  );
}
