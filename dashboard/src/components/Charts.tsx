import type { ReactNode } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatTime } from "./Indicators";

const AXIS = { stroke: "var(--muted-foreground)", fontSize: 10, fontFamily: "var(--font-mono)" };

const tooltipStyle = {
  contentStyle: {
    background: "var(--popover)",
    border: "1px solid var(--border)",
    borderRadius: "8px",
    fontSize: 12,
    fontFamily: "var(--font-mono)",
    color: "var(--foreground)",
  },
  labelStyle: { color: "var(--muted-foreground)", fontSize: 11 },
} as const;

function Frame({ children, height = 240 }: { children: ReactNode; height?: number | undefined }) {
  return (
    <div style={{ height }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        {children as never}
      </ResponsiveContainer>
    </div>
  );
}

export function MmdChart({
  data,
  threshold,
}: {
  data: { t: string; mmd2: number; threshold?: number | undefined }[];
  threshold?: number | undefined;
}) {
  return (
    <Frame height={260}>
      <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: -12 }}>
        <defs>
          <linearGradient id="mmdFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--telemetry)" stopOpacity={0.45} />
            <stop offset="100%" stopColor="var(--telemetry)" stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={formatTime}
          {...AXIS}
          tickLine={false}
          minTickGap={56}
          interval="preserveStartEnd"
        />
        <YAxis {...AXIS} tickLine={false} width={52} />
        <Tooltip {...tooltipStyle} labelFormatter={(v: string) => formatTime(v)} />
        {threshold !== undefined && (
          <ReferenceLine
            y={threshold}
            stroke="var(--warn)"
            strokeDasharray="5 4"
            label={{
              value: `threshold ${threshold}`,
              fill: "var(--warn)",
              fontSize: 10,
              position: "insideTopRight",
            }}
          />
        )}
        <Area
          type="monotone"
          dataKey="mmd2"
          stroke="var(--telemetry)"
          strokeWidth={1.8}
          fill="url(#mmdFill)"
        />
      </AreaChart>
    </Frame>
  );
}

export function RecallChart({
  data,
}: {
  data: { t: string; recall: number; lcb?: number | undefined }[];
}) {
  return (
    <Frame>
      <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: -12 }}>
        <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={formatTime}
          {...AXIS}
          tickLine={false}
          minTickGap={56}
          interval="preserveStartEnd"
        />
        <YAxis {...AXIS} tickLine={false} width={52} domain={["auto", "auto"]} />
        <Tooltip {...tooltipStyle} labelFormatter={(v: string) => formatTime(v)} />
        <Line
          type="monotone"
          dataKey="recall"
          stroke="var(--ok)"
          strokeWidth={1.8}
          dot={false}
          name="recall"
        />
        <Line
          type="monotone"
          dataKey="lcb"
          stroke="var(--telemetry)"
          strokeWidth={1.4}
          strokeDasharray="4 3"
          dot={false}
          name="recall LCB"
        />
      </LineChart>
    </Frame>
  );
}

export function LatencyChart({
  data,
}: {
  data: { t: string; meanMs: number; p95Ms: number; ucbMs?: number | undefined }[];
}) {
  return (
    <Frame>
      <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: -12 }}>
        <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={formatTime}
          {...AXIS}
          tickLine={false}
          minTickGap={56}
          interval="preserveStartEnd"
        />
        <YAxis {...AXIS} tickLine={false} width={52} unit="ms" />
        <Tooltip {...tooltipStyle} labelFormatter={(v: string) => formatTime(v)} />
        <Line
          type="monotone"
          dataKey="meanMs"
          stroke="var(--telemetry)"
          strokeWidth={1.6}
          dot={false}
          name="mean"
        />
        <Line
          type="monotone"
          dataKey="p95Ms"
          stroke="var(--warn)"
          strokeWidth={1.6}
          dot={false}
          name="p95"
        />
        <Line
          type="monotone"
          dataKey="ucbMs"
          stroke="var(--violet)"
          strokeWidth={1.3}
          strokeDasharray="4 3"
          dot={false}
          name="UCB"
        />
      </LineChart>
    </Frame>
  );
}

export function ThroughputChart({
  data,
}: {
  data: { t: string; qps: number; errorRate: number }[];
}) {
  return (
    <Frame>
      <BarChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: -12 }}>
        <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
        <XAxis
          dataKey="t"
          tickFormatter={formatTime}
          {...AXIS}
          tickLine={false}
          minTickGap={56}
          interval="preserveStartEnd"
        />
        <YAxis {...AXIS} tickLine={false} width={52} />
        <Tooltip {...tooltipStyle} labelFormatter={(v: string) => formatTime(v)} />
        <Bar dataKey="qps" fill="var(--telemetry-dim)" name="qps" radius={[2, 2, 0, 0]} />
      </BarChart>
    </Frame>
  );
}

export function ContributionBars({ items }: { items: { name: string; contribution: number }[] }) {
  const max = Math.max(...items.map((i) => i.contribution), 0.0001);
  return (
    <ul className="space-y-2.5">
      {items.map((item) => (
        <li key={item.name}>
          <div className="flex items-baseline justify-between gap-3">
            <span className="truncate font-mono text-xs text-foreground">{item.name}</span>
            <span className="font-mono text-[11px] text-muted-foreground">
              {(item.contribution * 100).toFixed(1)}%
            </span>
          </div>
          <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-surface-2">
            <div
              className="h-full rounded-full bg-telemetry/70"
              style={{ width: `${(item.contribution / max) * 100}%` }}
            />
          </div>
        </li>
      ))}
    </ul>
  );
}
