"use client";

import type { CashHistoryPoint } from "@/types/snapshot";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  ResponsiveContainer,
  ReferenceDot,
  Tooltip,
} from "recharts";

function formatYAxis(val: number): string {
  if (val >= 1e9) return `${(val / 1e9).toFixed(1)}B`;
  if (val >= 1e6) return `${(val / 1e6).toFixed(0)}M`;
  if (val >= 1e3) return `${(val / 1e3).toFixed(0)}K`;
  return String(val);
}

function CustomTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: CashHistoryPoint }> }) {
  if (!active || !payload?.length) return null;
  const point = payload[0].payload;
  return (
    <div className="bg-zinc-900 border border-white/10 rounded px-3 py-2 text-xs">
      <p className="text-zinc-400">{point.quarter}</p>
      <p className="text-zinc-100 font-medium">A${formatYAxis(point.cash_balance)}</p>
    </div>
  );
}

export function CashTrajectoryChart({
  data,
}: {
  data: CashHistoryPoint[];
}) {
  if (data.length < 3) return null;

  const markers = data.filter((d) => d.marker);
  const latest = data[data.length - 1];

  // XAxis ticks: first, middle, last
  const tickIndices = [0, Math.floor(data.length / 2), data.length - 1];
  const ticks = [...new Set(tickIndices.map((i) => data[i]?.quarter).filter(Boolean))];

  return (
    <div>
      <div className="flex items-baseline justify-between mb-4">
        <p className="text-xs uppercase tracking-wider text-zinc-500">
          Cash Trajectory
        </p>
        <p className="text-xs text-zinc-600">{data.length} quarters</p>
      </div>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="cashGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#a1a1aa" stopOpacity={0.15} />
                <stop offset="100%" stopColor="#a1a1aa" stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="quarter"
              axisLine={false}
              tickLine={false}
              tick={{ fill: "#71717a", fontSize: 11 }}
              ticks={ticks}
            />
            <YAxis
              orientation="right"
              axisLine={false}
              tickLine={false}
              tick={{ fill: "#71717a", fontSize: 11 }}
              tickFormatter={formatYAxis}
              width={52}
            />
            <Tooltip
              content={<CustomTooltip />}
              cursor={{ stroke: "rgba(255,255,255,0.06)" }}
            />
            <Area
              type="monotone"
              dataKey="cash_balance"
              stroke="#a1a1aa"
              strokeWidth={1.5}
              fill="url(#cashGradient)"
            />
            {/* Latest data point — filled white dot */}
            {latest && (
              <ReferenceDot
                x={latest.quarter}
                y={latest.cash_balance}
                r={4}
                fill="#fafafa"
                stroke="none"
              />
            )}
            {/* Capital event markers — open amber dots */}
            {markers.map((m) => (
              <ReferenceDot
                key={m.quarter}
                x={m.quarter}
                y={m.cash_balance}
                r={4}
                fill="transparent"
                stroke="#f5b642"
                strokeWidth={2}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
