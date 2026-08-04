import {
  Activity,
  Gauge,
  History,
  LayoutDashboard,
  Radar,
  Terminal,
  type LucideIcon,
} from "lucide-react";
import type { ReactNode } from "react";
import { SCENARIO_META, useScenario } from "@/app/scenario";
import { NavLink } from "@/components/NavLink";
import { DEMO_DATA_LABEL, DEMO_DATA_NOTICE } from "@/lib/demo-data";
import { DEMO_SCENARIOS } from "@/lib/types";
import { cn } from "@/lib/utils";

const NAV: { to: string; label: string; icon: LucideIcon; hint: string }[] = [
  { to: "/", label: "Overview", icon: LayoutDashboard, hint: "Mode, health, KPIs, events" },
  { to: "/drift", label: "Drift Intelligence", icon: Radar, hint: "MMD², KS/Holm, attribution" },
  { to: "/performance", label: "Performance", icon: Gauge, hint: "Recall, latency, SLO" },
  {
    to: "/canary",
    label: "Canary Operations",
    icon: Activity,
    hint: "Split, candidate, lifecycle",
  },
  {
    to: "/command-center",
    label: "Command Center",
    icon: Terminal,
    hint: "Control actions, safety gates",
  },
  { to: "/audit", label: "Audit & Evidence", icon: History, hint: "Records, hashes, verification" },
];

function ScenarioSelector() {
  const { scenario, setScenario } = useScenario();
  return (
    <div className="flex items-center gap-2">
      <label htmlFor="demo-scenario" className="label-mono hidden lg:block">
        Demo scenario
      </label>
      <select
        id="demo-scenario"
        value={scenario}
        onChange={(e) => setScenario(e.target.value as typeof scenario)}
        aria-describedby="demo-scenario-hint"
        className="rounded-md border border-border bg-surface-2 px-3 py-1.5 font-mono text-xs text-foreground outline-none transition-colors focus-visible:border-telemetry focus-visible:ring-2 focus-visible:ring-telemetry/40"
      >
        {DEMO_SCENARIOS.map((s) => (
          <option key={s} value={s}>
            {SCENARIO_META[s].label}
          </option>
        ))}
      </select>
      <span
        id="demo-scenario-hint"
        className="hidden max-w-[16rem] text-xs text-muted-foreground xl:block"
      >
        {SCENARIO_META[scenario].hint}
      </span>
    </div>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="grid-backdrop min-h-screen text-foreground">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded focus:bg-surface-3 focus:px-3 focus:py-2"
      >
        Skip to content
      </a>
      <header className="sticky top-0 z-30 border-b border-border/80 bg-background/85 backdrop-blur-xl">
        <div className="flex flex-wrap items-center justify-between gap-4 px-5 py-3 xl:px-8">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg border border-telemetry/40 bg-telemetry/10 font-mono text-xs font-semibold text-telemetry">
              VD
            </div>
            <div>
              <h1 className="text-sm font-semibold tracking-[0.06em] text-foreground">
                VD Control Center
              </h1>
              <p className="text-[11px] tracking-[0.05em] text-muted-foreground">
                Adaptive Milvus Tuning • Drift Detection • Canary Operations
              </p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <span
              title={DEMO_DATA_NOTICE}
              className="inline-flex items-center gap-1.5 rounded-full border border-warn/40 bg-warn/10 px-2.5 py-1 font-mono text-[11px] uppercase tracking-[0.1em] text-warn"
            >
              {DEMO_DATA_LABEL}
            </span>
            <ScenarioSelector />
          </div>
        </div>
      </header>

      <div className="mx-auto flex w-full max-w-[1800px] flex-col gap-6 px-5 py-6 md:flex-row xl:px-8">
        <nav aria-label="Primary" className="md:w-60 md:shrink-0">
          <ul className="flex gap-2 overflow-x-auto md:sticky md:top-24 md:flex-col md:overflow-visible">
            {NAV.map(({ to, label, icon: Icon, hint }) => (
              <li key={to} className="shrink-0 md:shrink">
                <NavLink
                  to={to}
                  exact={to === "/"}
                  className={cn(
                    "group flex items-center gap-3 rounded-lg border border-transparent px-3 py-2.5 text-sm text-muted-foreground transition-colors hover:border-border hover:bg-surface-2 hover:text-foreground",
                  )}
                  activeClassName="border-telemetry/40 bg-telemetry/10 text-telemetry hover:text-telemetry hover:border-telemetry/40"
                >
                  <Icon className="h-4 w-4 shrink-0" aria-hidden />
                  <span className="flex min-w-0 flex-col">
                    <span className="truncate font-medium">{label}</span>
                    <span className="hidden truncate text-[11px] text-muted-foreground md:block">
                      {hint}
                    </span>
                  </span>
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>

        <main id="main" className="min-w-0 flex-1 space-y-6 pb-16">
          {children}
        </main>
      </div>
    </div>
  );
}

export function PageHeader({
  title,
  description,
  aside,
}: {
  title: string;
  description: string;
  aside?: ReactNode | undefined;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <h2 className="text-lg font-semibold tracking-tight text-foreground">{title}</h2>
        <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{description}</p>
      </div>
      {aside}
    </div>
  );
}
