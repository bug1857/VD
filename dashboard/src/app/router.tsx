import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

/**
 * Minimal client-side router.
 *
 * This build is a pure browser SPA: there is no server, no SSR and no data
 * loaders. Navigation is History API only, so every view is reachable without
 * a backend.
 */

interface RouterValue {
  path: string;
  navigate: (to: string) => void;
}

const RouterContext = createContext<RouterValue | null>(null);

function normalize(path: string): string {
  if (path.length > 1 && path.endsWith("/")) return path.slice(0, -1);
  return path || "/";
}

export function RouterProvider({ children }: { children: ReactNode }) {
  const [path, setPath] = useState(() => normalize(window.location.pathname));

  useEffect(() => {
    const onPop = () => setPath(normalize(window.location.pathname));
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const value = useMemo<RouterValue>(
    () => ({
      path,
      navigate: (to: string) => {
        const next = normalize(to);
        if (next === normalize(window.location.pathname)) return;
        window.history.pushState({}, "", next);
        setPath(next);
        window.scrollTo({ top: 0 });
      },
    }),
    [path],
  );

  return <RouterContext.Provider value={value}>{children}</RouterContext.Provider>;
}

export function useRouter(): RouterValue {
  const ctx = useContext(RouterContext);
  if (!ctx) throw new Error("useRouter must be used inside <RouterProvider>");
  return ctx;
}
