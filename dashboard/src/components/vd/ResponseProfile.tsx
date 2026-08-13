import { useState } from "react";
import { responseProfile } from "@/lib/vd-data";

const R_MIN = 0.84;
const R_MAX = 1.0;
const L_MIN = 0;
const L_MAX = 72;

export function ResponseProfile() {
  const [hover, setHover] = useState<number | null>(null);
  const [selected, setSelected] = useState<number>(400);

  const pctR = (v: number) => ((v - R_MIN) / (R_MAX - R_MIN)) * 100;
  const pctL = (v: number) => ((v - L_MIN) / (L_MAX - L_MIN)) * 100;

  return (
    <section aria-label="Response profile">
      <div className="flex items-baseline justify-between">
        <h2 className="text-[13.5px] font-medium tracking-[-0.01em] text-ink-2">
          Predicted response profile
        </h2>
        <p className="text-[12.5px] text-ink-4">
          Prediction only — carries no authority to route. SIMULATED DATA.
        </p>
      </div>

      <div className="mt-6 grid grid-cols-[86px_1fr_1fr] gap-x-10">
        <div />
        <AxisHead title="Capped recall" left="0.84" right="1.00" note="mean with LCB–UCB" />
        <AxisHead title="p95 latency" left="0 ms" right="72 ms" note="mean with LCB–UCB" />
      </div>

      <div className="mt-2">
        {responseProfile.map((p) => {
          const isHover = hover === p.ef;
          const isSel = selected === p.ef;
          return (
            <div
              key={p.ef}
              onMouseEnter={() => setHover(p.ef)}
              onMouseLeave={() => setHover(null)}
              onClick={() => setSelected(p.ef)}
              className={[
                "grid cursor-pointer grid-cols-[86px_1fr_1fr] items-center gap-x-10 border-t border-line py-[22px] transition-colors duration-150",
                isHover ? "bg-hover/40" : "",
              ].join(" ")}
            >
              <div className="pl-1">
                <div className="flex items-baseline gap-2">
                  <span
                    className={[
                      "mono text-[15.5px] tabular-nums",
                      p.role ? "text-ink" : "text-ink-2",
                    ].join(" ")}
                  >
                    {p.ef}
                  </span>
                  {isSel && <span className="h-1 w-1 rounded-full bg-accent" />}
                </div>
                <span className="text-[12px] text-ink-4">
                  {p.role === "serving"
                    ? "serving · LKG"
                    : p.role === "candidate"
                      ? "candidate"
                      : "evaluated"}
                </span>
              </div>

              <Interval
                lcb={pctR(p.recallLcb)}
                ucb={pctR(p.recallUcb)}
                mean={pctR(p.recall)}
                role={p.role}
                emphasize={isHover || isSel}
                readout={`${p.recall.toFixed(3)}`}
                sub={`${p.recallLcb.toFixed(3)} – ${p.recallUcb.toFixed(3)}`}
              />

              <Interval
                lcb={pctL(p.p95Lcb)}
                ucb={pctL(p.p95Ucb)}
                mean={pctL(p.p95)}
                role={p.role}
                emphasize={isHover || isSel}
                readout={`${p.p95.toFixed(1)} ms`}
                sub={`${p.p95Lcb.toFixed(1)} – ${p.p95Ucb.toFixed(1)} ms`}
              />
            </div>
          );
        })}
      </div>

      <p className="mt-5 border-t border-line pt-4 text-[13px] leading-[1.6] text-ink-3">
        ef 400 is the serving and last-known-good configuration. ef 800 shows higher predicted
        recall at roughly 1.7× predicted p95 latency; that prediction is qualified and admitted, and
        remains unauthorized.
      </p>
    </section>
  );
}

function AxisHead({
  title,
  left,
  right,
  note,
}: {
  title: string;
  left: string;
  right: string;
  note: string;
}) {
  return (
    <div className="pr-[132px]">
      <div className="flex items-baseline justify-between">
        <span className="whitespace-nowrap text-[13px] text-ink-2">{title}</span>
        <span className="whitespace-nowrap pl-4 text-[12px] text-ink-4">{note}</span>
      </div>
      <div className="mt-2 flex items-baseline justify-between text-[11.5px] text-ink-4">
        <span className="mono">{left}</span>
        <span className="mono">{right}</span>
      </div>
    </div>
  );
}

function Interval({
  lcb,
  ucb,
  mean,
  role,
  emphasize,
  readout,
  sub,
}: {
  lcb: number;
  ucb: number;
  mean: number;
  role?: "serving" | "candidate" | undefined;
  emphasize: boolean;
  readout: string;
  sub: string;
}) {
  return (
    <div className="flex items-center gap-5">
      <div className="relative h-6 flex-1">
        <span className="absolute inset-x-0 top-1/2 h-px -translate-y-1/2 bg-line" />
        <span
          className={[
            "absolute top-1/2 h-[3px] -translate-y-1/2 rounded-full transition-opacity duration-150",
            role === "candidate"
              ? "bg-accent/45"
              : role === "serving"
                ? "bg-ink-2/45"
                : "bg-ink-4/40",
            emphasize ? "opacity-100" : "opacity-80",
          ].join(" ")}
          style={{ left: `${lcb}%`, width: `${Math.max(ucb - lcb, 0.6)}%` }}
        />
        <span
          className={[
            "absolute top-1/2 h-[13px] w-px -translate-y-1/2 transition-colors duration-150",
            role === "candidate" ? "bg-accent" : role === "serving" ? "bg-ink" : "bg-ink-3",
          ].join(" ")}
          style={{ left: `${mean}%` }}
        />
      </div>
      <div className="w-[112px] shrink-0 text-right">
        <div className="mono tabular-nums text-[13px] text-ink-2">{readout}</div>
        <div className="mono whitespace-nowrap tabular-nums text-[11.5px] text-ink-4">{sub}</div>
      </div>
    </div>
  );
}
