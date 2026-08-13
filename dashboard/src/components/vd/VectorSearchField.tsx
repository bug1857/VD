import { useEffect, useRef, useState } from "react";

/**
 * Vector Search Field — a live depiction of ANN search over an HNSW index.
 *
 * Reads as: an embedding point cloud with natural cluster structure, one active
 * query vector, a range/threshold radius, greedy graph traversal lighting a
 * small number of directed hops, the serving search breadth (ef 400) as a
 * restrained frontier, and a candidate breadth (ef 800) that briefly previews a
 * wider frontier before receding — never resolving into a served state.
 *
 * SIMULATED DATA. Decorative only; establishes no authority.
 */

type Node = {
  x: number;
  y: number;
  cl: number;
  jx: number;
  jy: number;
  ph: number;
};

const SERVING_VISITS = 18;
const CANDIDATE_VISITS = 34;
const RETURNED = 6;

const CYCLE = 7400;
const P_QUERY = 620; // query appears
const P_TRAVERSE = 2050; // serving traversal completes
const P_CAND = 3250; // candidate preview fully expanded
const P_RECEDE = 4150; // candidate receded, neighbours resolved

export function VectorSearchField() {
  const ref = useRef<HTMLCanvasElement | null>(null);
  const [phaseLabel, setPhaseLabel] = useState("traversing");

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const reduced = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches;

    let width = 0;
    let height = 0;
    let raf = 0;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);

    const resize = () => {
      const r = canvas.getBoundingClientRect();
      width = r.width;
      height = r.height;
      canvas.width = Math.max(1, Math.floor(width * dpr));
      canvas.height = Math.max(1, Math.floor(height * dpr));
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };
    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(canvas);

    const rnd = mulberry(20260809);

    // --- clustered embedding cloud (not a sphere, not a lattice) ---
    const centers = [
      [0.24, 0.3],
      [0.44, 0.62],
      [0.63, 0.26],
      [0.72, 0.68],
      [0.88, 0.44],
      [0.14, 0.72],
    ] as const;
    const counts = [52, 62, 44, 58, 34, 30];

    const nodes: Node[] = [];
    centers.forEach(([cx, cy], ci) => {
      const sx = 0.075 + rnd() * 0.05;
      const sy = 0.085 + rnd() * 0.05;
      for (let i = 0; i < counts[ci]!; i++) {
        nodes.push({
          x: clamp(cx + gauss(rnd) * sx, 0.04, 0.97),
          y: clamp(cy + gauss(rnd) * sy, 0.05, 0.95),
          cl: ci,
          jx: 0,
          jy: 0,
          ph: rnd() * Math.PI * 2,
        });
      }
    });
    // sparse interstitial points so clusters are not islands
    for (let i = 0; i < 26; i++) {
      nodes.push({
        x: 0.08 + rnd() * 0.88,
        y: 0.07 + rnd() * 0.86,
        cl: -1,
        jx: 0,
        jy: 0,
        ph: rnd() * Math.PI * 2,
      });
    }

    const N = nodes.length;
    const ASPECT = 0.3; // vertical squash used consistently for distance + draw
    const d2 = (a: { x: number; y: number }, b: { x: number; y: number }) => {
      const dx = a.x - b.x;
      const dy = (a.y - b.y) * ASPECT;
      return dx * dx + dy * dy;
    };

    // --- proximity graph (HNSW-like base layer, k = 5) ---
    const K = 6;
    const adj: number[][] = nodes.map((n, i) => {
      const order = nodes
        .map((m, j) => ({ j, d: j === i ? Infinity : d2(n, m) }))
        .sort((a, b) => a.d - b.d)
        .slice(0, K)
        .map((o) => o.j);
      return order;
    });
    // a few long-range links, standing in for upper-layer shortcuts
    for (let i = 0; i < 14; i++) {
      const a = Math.floor(rnd() * N);
      const b = Math.floor(rnd() * N);
      if (a !== b) adj[a]!.push(b);
    }
    // symmetrize lightly
    adj.forEach((list, i) =>
      list.forEach((j) => {
        if (!adj[j]!.includes(i) && adj[j]!.length < K + 2) adj[j]!.push(i);
      }),
    );

    type Search = {
      q: { x: number; y: number };
      radius: number;
      order: number[]; // visit order, greedy best-first
      hops: [number, number][]; // directed edges in visit order
      returned: number[];
    };

    const buildSearch = (seed: number): Search => {
      const r = mulberry(seed);
      const c = centers[Math.floor(r() * centers.length)]!;
      const q = {
        x: clamp(c[0] + gauss(r) * 0.06, 0.12, 0.9),
        y: clamp(c[1] + gauss(r) * 0.06, 0.14, 0.86),
      };
      // entry at moderate distance so greedy traversal visibly converges
      const byDist = nodes
        .map((n, i) => ({ i, d: d2(n, q) }))
        .sort((a, b) => a.d - b.d);
      const entry = byDist[Math.floor(N * (0.42 + r() * 0.16))]!.i;

      const visited = new Set<number>([entry]);
      const fullOrder = [entry];
      const parent = new Map<number, number>();
      const frontier: { i: number; d: number; from: number }[] = [];
      adj[entry]!.forEach((j) =>
        frontier.push({ i: j, d: d2(nodes[j]!, q), from: entry }),
      );

      const trueNearest = byDist[0]!.i;
      let nearestAt = -1;
      while (fullOrder.length < 400 && frontier.length) {
        frontier.sort((a, b) => a.d - b.d);
        const next = frontier.shift()!;
        if (visited.has(next.i)) continue;
        visited.add(next.i);
        fullOrder.push(next.i);
        parent.set(next.i, next.from);
        if (nearestAt < 0 && next.i === trueNearest)
          nearestAt = fullOrder.length - 1;
        // keep walking a little past the best neighbour so the candidate
        // frontier has room to widen around it
        if (nearestAt >= 0 && fullOrder.length > nearestAt + (CANDIDATE_VISITS - SERVING_VISITS) + 4)
          break;
        adj[next.i]!.forEach((j) => {
          if (!visited.has(j))
            frontier.push({ i: j, d: d2(nodes[j]!, q), from: next.i });
        });
      }

      // keep the window of the traversal that reads as "search breadth around
      // the query": the serving slice must already contain the best neighbour,
      // the candidate slice then widens the frontier around it
      const idxN = nearestAt >= 0 ? nearestAt : fullOrder.length - 1;
      const startIdx = Math.max(0, idxN - (SERVING_VISITS - 3));
      const order = fullOrder.slice(startIdx, startIdx + CANDIDATE_VISITS);
      const inOrder = new Set(order);
      const hops: [number, number][] = [];
      order.forEach((i) => {
        const from = parent.get(i);
        if (from !== undefined && inOrder.has(from)) hops.push([from, i]);
      });

      const returned = [...order.slice(0, SERVING_VISITS)]
        .sort((a, b) => d2(nodes[a]!, q) - d2(nodes[b]!, q))
        .slice(0, RETURNED);

      const radius = Math.sqrt(d2(nodes[returned[RETURNED - 1]!]!, q)) * 1.22;
      return { q, radius, order, hops, returned };
    };

    let cycle = 0;
    let search = buildSearch(1001);
    const start = performance.now();
    let last = start;
    let labelState = "";

    const setLabel = (s: string) => {
      if (s !== labelState) {
        labelState = s;
        setPhaseLabel(s);
      }
    };

    // uniform projection: one data unit maps to `scale` px in x and
    // `scale * ASPECT` px in y — the same weighting used by d2()
    const projScale = () => Math.min(width - 36, (height - 36) / ASPECT);
    const px = (n: { x: number; y: number }, _pad: number) =>
      width / 2 + (n.x - 0.5) * projScale();
    const py = (n: { x: number; y: number }, _pad: number) =>
      height / 2 + (n.y - 0.5) * projScale() * ASPECT;

    const draw = (now: number) => {
      const dt = Math.min(now - last, 48);
      last = now;
      const elapsed = now - start;
      const c = Math.floor(elapsed / CYCLE);
      if (c !== cycle) {
        cycle = c;
        search = buildSearch(1001 + c * 977);
      }
      const t = elapsed % CYCLE;
      const time = now / 1000;

      const pad = 18;
      ctx.clearRect(0, 0, width, height);

      const scale = projScale();
      const qx = px(search.q, pad);
      const qy = py(search.q, pad);

      // phase progressions
      const appear = ease(clamp01(t / P_QUERY));
      const trav = clamp01((t - P_QUERY * 0.55) / (P_TRAVERSE - P_QUERY * 0.55));
      const cand = clamp01((t - P_TRAVERSE) / (P_CAND - P_TRAVERSE));
      const recede = clamp01((t - P_CAND) / (P_RECEDE - P_CAND));
      const settle = clamp01((t - P_RECEDE) / (CYCLE - P_RECEDE));
      const fadeOut = ease(clamp01((t - (CYCLE - 900)) / 900));

      if (t < P_TRAVERSE) setLabel("traversing · ef 400");
      else if (t < P_RECEDE) setLabel("candidate preview · ef 800");
      else setLabel("neighbours returned");

      const servingCount = Math.floor(ease(trav) * SERVING_VISITS);
      const candCount =
        SERVING_VISITS +
        Math.floor(
          ease(cand) * (CANDIDATE_VISITS - SERVING_VISITS) * (1 - recede),
        );

      const visitRank = new Map<number, number>();
      search.order.forEach((i, r) => visitRank.set(i, r));
      const returnedSet = new Set(search.returned);

      // measurement sweep during recomputation cycles
      const recomputing = cycle % 3 === 2;
      const sweepX = recomputing
        ? pad + (((t / CYCLE) * 1.25 - 0.1) % 1.25) * (width - pad * 2)
        : -9999;

      // --- ambient drift (very subtle) ---
      for (const n of nodes) {
        n.jx = Math.sin(time * 0.16 + n.ph) * 0.0016;
        n.jy = Math.cos(time * 0.13 + n.ph * 1.3) * 0.0014;
      }

      // --- query radius / threshold boundary ---
      const rr = search.radius * scale;
      ctx.save();
      ctx.setLineDash([2, 6]);
      ctx.lineWidth = 1;
      ctx.strokeStyle = `rgba(243,240,236,${0.1 * appear * (1 - fadeOut)})`;
      ctx.beginPath();
      ctx.ellipse(
        qx,
        qy,
        rr * (0.94 + 0.06 * appear),
        rr * ASPECT * (0.94 + 0.06 * appear),
        0,
        0,
        Math.PI * 2,
      );
      ctx.stroke();
      ctx.restore();

      // --- traversal edges ---
      search.hops.forEach(([a, b], idx) => {
        const isCandidate = idx + 1 >= SERVING_VISITS;
        const shown = isCandidate ? idx + 1 < candCount : idx + 1 < servingCount;
        if (!shown) return;
        const age = (isCandidate ? candCount : servingCount) - (idx + 1);
        const fresh = clamp01(1 - age / 6);
        const na = nodes[a]!;
        const nb = nodes[b]!;
        const ax = px({ x: na.x + na.jx, y: na.y + na.jy }, pad);
        const ay = py({ x: na.x + na.jx, y: na.y + na.jy }, pad);
        const bx = px({ x: nb.x + nb.jx, y: nb.y + nb.jy }, pad);
        const by = py({ x: nb.x + nb.jx, y: nb.y + nb.jy }, pad);

        ctx.beginPath();
        ctx.lineWidth = isCandidate ? 0.7 : 0.9;
        if (isCandidate) {
          ctx.setLineDash([2.5, 3.5]);
          ctx.strokeStyle = `rgba(147,151,196,${(0.1 + fresh * 0.24) * (1 - recede) * (1 - fadeOut)})`;
        } else {
          ctx.setLineDash([]);
          ctx.strokeStyle = `rgba(226,224,232,${(0.09 + fresh * 0.3) * (1 - fadeOut * 0.7)})`;
        }
        ctx.moveTo(ax, ay);
        ctx.lineTo(bx, by);
        ctx.stroke();
        ctx.setLineDash([]);

        // small directional tick near the head of the freshest hops
        if (fresh > 0.55 && !isCandidate) {
          const mx = bx + (ax - bx) * 0.26;
          const my = by + (ay - by) * 0.26;
          ctx.beginPath();
          ctx.fillStyle = `rgba(243,240,236,${(fresh - 0.55) * 0.6})`;
          ctx.arc(mx, my, 0.9, 0, Math.PI * 2);
          ctx.fill();
        }
      });

      // --- points ---
      for (let i = 0; i < N; i++) {
        const n = nodes[i]!;
        const x = px({ x: n.x + n.jx, y: n.y + n.jy }, pad);
        const y = py({ x: n.x + n.jx, y: n.y + n.jy }, pad);

        const rank = visitRank.get(i);
        const inServing = rank !== undefined && rank < servingCount;
        const inCandidate =
          rank !== undefined && rank >= SERVING_VISITS && rank < candCount;
        const isReturned =
          returnedSet.has(i) && t > P_TRAVERSE * 0.9;

        const active = inServing || inCandidate;
        const dim = active ? 0 : ease(clamp01((t - P_QUERY) / 900)) * 0.45;

        let alpha = 0.24 - dim * 0.1;
        let size = 1.05;
        let color = "243,240,236";

        if (inServing) {
          alpha = 0.5;
          size = 1.45;
        }
        if (inCandidate) {
          alpha = 0.34 * (1 - recede);
          size = 1.3;
          color = "147,151,196";
        }
        if (isReturned) {
          const g = ease(clamp01((t - P_TRAVERSE * 0.9) / 700));
          alpha = 0.45 + g * 0.5;
          size = 1.5 + g * 0.95;
        }

        // measurement sweep brightens points it crosses
        if (recomputing) {
          const d = Math.abs(x - sweepX);
          if (d < 46) alpha += (1 - d / 46) * 0.22;
        }

        alpha *= 1 - fadeOut * 0.35;

        ctx.beginPath();
        ctx.fillStyle = `rgba(${color},${Math.max(0.05, alpha)})`;
        ctx.arc(x, y, size, 0, Math.PI * 2);
        ctx.fill();

        if (isReturned) {
          ctx.beginPath();
          ctx.strokeStyle = `rgba(243,240,236,0.22)`;
          ctx.lineWidth = 0.7;
          ctx.arc(x, y, size + 3.2, 0, Math.PI * 2);
          ctx.stroke();
        }
      }

      // --- query vector marker ---
      const qa = appear * (1 - fadeOut);
      ctx.strokeStyle = `rgba(243,240,236,${0.65 * qa})`;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(qx - 5, qy);
      ctx.lineTo(qx + 5, qy);
      ctx.moveTo(qx, qy - 5);
      ctx.lineTo(qx, qy + 5);
      ctx.stroke();
      ctx.beginPath();
      ctx.fillStyle = `rgba(243,240,236,${0.9 * qa})`;
      ctx.arc(qx, qy, 1.9, 0, Math.PI * 2);
      ctx.fill();

      // --- in-field labels: query · visited · top-k ---
      ctx.font =
        '11px ui-monospace, "Geist Mono", SFMono-Regular, monospace';
      ctx.textBaseline = "alphabetic";
      ctx.fillStyle = `rgba(243,240,236,${0.55 * qa})`;
      ctx.fillText("query", qx + 9, qy - 7);

      if (servingCount > 2) {
        const vIdx = search.order[Math.min(servingCount - 1, search.order.length - 1)]!;
        const vn = nodes[vIdx]!;
        ctx.fillStyle = `rgba(226,224,232,${0.42 * ease(trav) * (1 - fadeOut)})`;
        ctx.fillText(
          "visited · ef 400",
          px(vn, pad) + 8,
          py(vn, pad) - 6,
        );
      }
      if (candCount > SERVING_VISITS + 1) {
        const cIdx = search.order[Math.min(candCount - 1, search.order.length - 1)]!;
        const cn = nodes[cIdx]!;
        ctx.fillStyle = `rgba(147,151,196,${0.5 * (1 - recede) * (1 - fadeOut)})`;
        ctx.fillText(
          "candidate · ef 800",
          px(cn, pad) + 8,
          py(cn, pad) - 6,
        );
      }
      if (t > P_TRAVERSE * 0.9) {
        const g = ease(clamp01((t - P_TRAVERSE * 0.9) / 700));
        ctx.fillStyle = `rgba(243,240,236,${0.45 * g * (1 - fadeOut)})`;
        ctx.fillText(
          `top-k ${RETURNED}`,
          qx - rr - 4,
          qy + rr * ASPECT + 14,
        );
      }


      // sweep line
      if (recomputing && sweepX > pad && sweepX < width - pad) {
        const g = ctx.createLinearGradient(sweepX - 40, 0, sweepX, 0);
        g.addColorStop(0, "rgba(147,151,196,0)");
        g.addColorStop(1, "rgba(147,151,196,0.20)");
        ctx.strokeStyle = g;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(sweepX, pad * 0.6);
        ctx.lineTo(sweepX, height - pad * 0.6);
        ctx.stroke();
      }

      void dt;
      void settle;
      raf = requestAnimationFrame(draw);
    };

    if (reduced) {
      // single representative frame: traversal complete, neighbours resolved
      const fixed = performance.now();
      Object.defineProperty(window, "__vsf", { value: fixed, writable: true });
      draw(fixed + P_RECEDE + 400);
      cancelAnimationFrame(raf);
      setPhaseLabel("neighbours returned");
    } else {
      raf = requestAnimationFrame(draw);
    }

    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
  }, []);

  return (
    <figure className="m-0">
      <figcaption className="flex items-baseline justify-between">
        <h2 className="text-[13.5px] font-medium tracking-[-0.01em] text-ink-2">
          Vector search field
        </h2>
        <span className="mono text-[11.5px] tracking-[0.02em] text-ink-4">
          {phaseLabel}
        </span>
      </figcaption>

      <p className="mt-2 max-w-[54ch] text-[13px] leading-[1.62] text-ink-3">
        Greedy traversal of the HNSW base layer toward one query vector. The
        solid frontier is the serving breadth. The dashed indigo expansion is
        candidate breadth, previewed only — it never reaches a served state.
      </p>

      <div className="relative mt-4 h-[286px] w-full overflow-hidden">
        <canvas
          ref={ref}
          className="h-full w-full"
          role="img"
          aria-label="Simulated HNSW nearest-neighbour traversal over a clustered embedding field"
        />
      </div>

      {/* ef ladder — search breadth, with serving and candidate marked */}
      <div className="mt-4 border-t border-line pt-4">
        <div className="flex items-baseline justify-between">
          <span className="text-[12.5px] text-ink-4">
            search breadth · ef ladder
          </span>
          <span className="mono text-[11.5px] text-ink-4">HNSW · M 32</span>
        </div>
        <div className="mt-3 grid grid-cols-4 gap-x-3">
          {[200, 400, 800, 1600].map((ef) => {
            const serving = ef === 400;
            const candidate = ef === 800;
            return (
              <div key={ef} className="flex flex-col gap-1.5">
                <span
                  className={[
                    "h-[3px] w-full rounded-[1px]",
                    serving
                      ? "bg-ink-2"
                      : candidate
                        ? "bg-accent/55"
                        : "bg-ink-4/30",
                  ].join(" ")}
                  style={
                    candidate
                      ? {
                          backgroundImage:
                            "repeating-linear-gradient(90deg, color-mix(in oklab, var(--accent) 60%, transparent) 0 5px, transparent 5px 9px)",
                          backgroundColor: "transparent",
                        }
                      : undefined
                  }
                />
                <span
                  className={[
                    "mono text-[12px] tabular-nums",
                    serving
                      ? "text-ink"
                      : candidate
                        ? "text-accent"
                        : "text-ink-4",
                  ].join(" ")}
                >
                  {ef}
                </span>
                <span className="text-[11.5px] text-ink-4">
                  {serving
                    ? "serving · LKG"
                    : candidate
                      ? "candidate"
                      : "evaluated"}
                </span>
              </div>
            );
          })}
        </div>
      </div>

      <dl className="mt-5 grid grid-cols-2 gap-x-10 gap-y-1.5 border-t border-line pt-3.5 text-[12.5px]">
        <Row k="returned" v={`top-k ${RETURNED}`} />
        <Row k="candidate state" v="preview · unauthorized" muted />
        <Row k="sampled queries" v="1,200 / 15 min" />
        <Row k="evidence" v="SIMULATED DATA" muted />
      </dl>

    </figure>
  );
}

function Row({ k, v, muted }: { k: string; v: string; muted?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-4 tabular-nums">
      <dt className="text-ink-4">{k}</dt>
      <dd className={`mono ${muted ? "text-ink-3" : "text-ink-2"}`}>{v}</dd>
    </div>
  );
}

function clamp(v: number, a: number, b: number) {
  return Math.min(b, Math.max(a, v));
}
function clamp01(v: number) {
  return clamp(v, 0, 1);
}
function ease(v: number) {
  return 1 - Math.pow(1 - v, 3);
}
function gauss(r: () => number) {
  return (r() + r() + r() + r() - 2) * 0.9;
}

function mulberry(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
