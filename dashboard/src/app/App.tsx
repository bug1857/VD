import { lazy, Suspense, useEffect, type ComponentType } from "react";
import { RouterProvider, useRouter } from "./router";
import { ScenarioProvider } from "./scenario";
import { AppShell } from "@/components/AppShell";
import { NavLink } from "@/components/NavLink";

const OverviewPage = lazy(async () => {
  const module = await import("@/pages/OverviewPage");
  return { default: module.OverviewPage };
});
const DriftPage = lazy(async () => {
  const module = await import("@/pages/DriftPage");
  return { default: module.DriftPage };
});
const PerformancePage = lazy(async () => {
  const module = await import("@/pages/PerformancePage");
  return { default: module.PerformancePage };
});
const CanaryPage = lazy(async () => {
  const module = await import("@/pages/CanaryPage");
  return { default: module.CanaryPage };
});
const CommandCenterPage = lazy(async () => {
  const module = await import("@/pages/CommandCenterPage");
  return { default: module.CommandCenterPage };
});
const AuditPage = lazy(async () => {
  const module = await import("@/pages/AuditPage");
  return { default: module.AuditPage };
});

interface RouteDef {
  path: string;
  title: string;
  description: string;
  component: ComponentType;
}

export const ROUTES: RouteDef[] = [
  {
    path: "/",
    title: "Overview — VD Control Center",
    description:
      "Operational overview of adaptive Milvus tuning: mode, health, connectivity, drift and recall KPIs, and the recorded event timeline.",
    component: OverviewPage,
  },
  {
    path: "/drift",
    title: "Drift Intelligence — VD Control Center",
    description:
      "MMD² kernel drift against threshold, KS/Holm feature rejections, breach history and drift explainability.",
    component: DriftPage,
  },
  {
    path: "/performance",
    title: "Performance — VD Control Center",
    description:
      "Recall with lower confidence bound, mean and p95 latency with upper bound, throughput, errors and the SLO matrix.",
    component: PerformancePage,
  },
  {
    path: "/canary",
    title: "Canary Operations — VD Control Center",
    description:
      "Data-derived last-known-good and candidate routing split, candidate evidence, lifecycle, schedule and outbox.",
    component: CanaryPage,
  },
  {
    path: "/command-center",
    title: "Command Center — VD Control Center",
    description:
      "Control actions with exact targets and confirmation requirements. Submission is disabled: backend not connected.",
    component: CommandCenterPage,
  },
  {
    path: "/audit",
    title: "Audit & Evidence — VD Control Center",
    description:
      "Searchable audit records, payload hashes, verification status, experiments and source revision.",
    component: AuditPage,
  },
];

function NotFound({ path }: { path: string }) {
  return (
    <div className="panel p-8 text-center">
      <p className="label-mono text-muted-foreground">404</p>
      <h2 className="mt-3 text-xl font-semibold text-foreground">No such view</h2>
      <p className="mt-2 text-sm text-muted-foreground">
        <span className="font-mono">{path}</span> is not part of the control center.
      </p>
      <NavLink
        to="/"
        className="mt-6 inline-flex items-center justify-center rounded-md border border-telemetry/40 bg-telemetry/10 px-4 py-2 text-sm font-medium text-telemetry transition-colors hover:bg-telemetry/20"
      >
        Back to overview
      </NavLink>
    </div>
  );
}

function setMeta(selector: string, attr: "name" | "property", key: string, content: string) {
  let el = document.head.querySelector<HTMLMetaElement>(selector);
  if (!el) {
    el = document.createElement("meta");
    el.setAttribute(attr, key);
    document.head.appendChild(el);
  }
  el.setAttribute("content", content);
}

function CurrentView() {
  const { path } = useRouter();
  const route = ROUTES.find((r) => r.path === path);

  useEffect(() => {
    const title = route?.title ?? "Not found — VD Control Center";
    const description =
      route?.description ?? "The requested view does not exist in the VD Control Center.";
    document.title = title;
    setMeta('meta[name="description"]', "name", "description", description);
    setMeta('meta[property="og:title"]', "property", "og:title", title);
    setMeta('meta[property="og:description"]', "property", "og:description", description);
  }, [route]);

  if (!route) return <NotFound path={path} />;
  const View = route.component;
  return (
    <Suspense
      fallback={
        <div className="panel p-8" role="status" aria-live="polite">
          <p className="label-mono">Loading dashboard view</p>
        </div>
      }
    >
      <View />
    </Suspense>
  );
}

export function App() {
  return (
    <RouterProvider>
      <ScenarioProvider>
        <AppShell>
          <CurrentView />
        </AppShell>
      </ScenarioProvider>
    </RouterProvider>
  );
}
