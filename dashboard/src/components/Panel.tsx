import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function Panel({
  title,
  subtitle,
  actions,
  children,
  className,
  bodyClassName,
}: {
  title?: string | undefined;
  subtitle?: string | undefined;
  actions?: ReactNode | undefined;
  children: ReactNode;
  className?: string | undefined;
  bodyClassName?: string | undefined;
}) {
  return (
    <section className={cn("panel", className)}>
      {(title || actions) && (
        <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border/70 px-5 py-3.5">
          <div>
            {title && (
              <h2 className="text-sm font-semibold tracking-wide text-foreground">{title}</h2>
            )}
            {subtitle && <p className="mt-0.5 text-xs text-muted-foreground">{subtitle}</p>}
          </div>
          {actions && <div className="flex items-center gap-2">{actions}</div>}
        </header>
      )}
      <div className={cn("px-5 py-4", bodyClassName)}>{children}</div>
    </section>
  );
}

export function Label({ children }: { children: ReactNode }) {
  return <span className="label-mono">{children}</span>;
}

export function KeyValue({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="min-w-0">
      <div className="label-mono">{label}</div>
      <div className="mt-1 truncate text-sm text-foreground">{value ?? "—"}</div>
    </div>
  );
}

export function Mono({
  children,
  className,
}: {
  children: ReactNode;
  className?: string | undefined;
}) {
  return (
    <span className={cn("font-mono text-xs text-muted-foreground", className)}>{children}</span>
  );
}
