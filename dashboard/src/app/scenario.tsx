import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import type { ApiEnvelope, DemoScenario } from "@/lib/types";

interface ScenarioContextValue {
  scenario: DemoScenario;
  setScenario: (scenario: DemoScenario) => void;
}

const ScenarioContext = createContext<ScenarioContextValue | null>(null);

export function ScenarioProvider({ children }: { children: ReactNode }) {
  const [scenario, setScenario] = useState<DemoScenario>("normal");
  const value = useMemo(() => ({ scenario, setScenario }), [scenario]);
  return <ScenarioContext.Provider value={value}>{children}</ScenarioContext.Provider>;
}

export function useScenario(): ScenarioContextValue {
  const ctx = useContext(ScenarioContext);
  if (!ctx) throw new Error("useScenario must be used inside <ScenarioProvider>");
  return ctx;
}

export const SCENARIO_META: Record<DemoScenario, { label: string; hint: string }> = {
  normal: { label: "Normal", hint: "Fresh demo telemetry, connected adapter" },
  loading: { label: "Loading", hint: "Reads never settle — skeleton states" },
  stale: { label: "Stale", hint: "Observations outside freshness TTL" },
  disconnected: { label: "Disconnected", hint: "Backend not connected" },
  blocked: { label: "Blocked", hint: "Suppressed by a required safety gate" },
  unauthorized: { label: "Unauthorized", hint: "Operator grant not verified" },
  empty: { label: "Empty", hint: "No observations in the window" },
  error: { label: "Error", hint: "Upstream read failure" },
};

export type ResourceState<T> =
  | { kind: "loading" }
  | { kind: "error"; envelope: ApiEnvelope<T> }
  | { kind: "empty"; envelope: ApiEnvelope<T> }
  | { kind: "ready"; envelope: ApiEnvelope<T>; data: T };

/**
 * Reads a demo envelope for the active scenario and classifies it into an
 * explicit UI state. Never fabricates data when the envelope is not ok.
 */
export function useResource<T>(
  load: (scenario: DemoScenario) => Promise<ApiEnvelope<T>>,
  isEmpty: (data: T) => boolean,
  deps: readonly unknown[] = [],
): { state: ResourceState<T>; reload: () => void } {
  const { scenario } = useScenario();
  const [state, setState] = useState<ResourceState<T>>({ kind: "loading" });
  const [nonce, setNonce] = useState(0);
  const reload = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });
    void load(scenario).then((envelope) => {
      if (cancelled) return;
      if (!envelope.ok || envelope.data === undefined) {
        setState({ kind: "error", envelope });
        return;
      }
      if (isEmpty(envelope.data)) {
        setState({ kind: "empty", envelope });
        return;
      }
      setState({ kind: "ready", envelope, data: envelope.data });
    });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scenario, nonce, ...deps]);

  return { state, reload };
}
