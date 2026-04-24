"use client";

import { useState, useRef, useCallback } from "react";
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

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload: CashHistoryPoint }>;
}) {
  if (!active || !payload?.length) return null;
  const point = payload[0].payload;
  return (
    <div className="bg-zinc-900 border border-white/10 rounded px-3 py-2 text-xs">
      <p className="text-zinc-400">{point.quarter}</p>
      <p className="text-zinc-100 font-medium">
        A${formatYAxis(point.cash_balance)}
      </p>
    </div>
  );
}

function BurnStrip({ data }: { data: CashHistoryPoint[] }) {
  const burnPoints = data.filter((d) => d.burn != null && d.burn_display != null);
  if (burnPoints.length < 2) return null;

  const maxBurn = Math.max(...burnPoints.map((d) => d.burn!));
  const [hovered, setHovered] = useState<number | null>(null);
  const stripRef = useRef<HTMLDivElement>(null);

  const latestBurn = burnPoints[burnPoints.length - 1];

  const handleEnter = useCallback((i: number) => setHovered(i), []);
  const handleLeave = useCallback(() => setHovered(null), []);

  return (
    <div className="mt-1" ref={stripRef}>
      <div className="flex items-baseline justify-between mb-1.5">
        <p className="text-[10px] uppercase tracking-wider text-zinc-600">
          Burn / quarter
        </p>
        {latestBurn.burn_display && (
          <p className="text-[10px] text-zinc-500">
            latest {latestBurn.burn_display}
          </p>
        )}
      </div>
      <div className="relative h-9 flex items-end gap-px">
        {burnPoints.map((point, i) => {
          const isLast = i === burnPoints.length - 1;
          const isHovered = hovered === i;
          const heightPct =
            maxBurn > 0 ? Math.max((point.burn! / maxBurn) * 100, 4) : 4;

          return (
            <div
              key={point.quarter}
              className="relative flex-1 flex items-end"
              onMouseEnter={() => handleEnter(i)}
              onMouseLeave={handleLeave}
            >
              <div
                className="w-full rounded-sm transition-opacity duration-[120ms] ease-in-out"
                style={{
                  height: `${heightPct}%`,
                  backgroundColor: isHovered
                    ? "rgba(250,250,250,1)"
                    : isLast
                      ? "rgba(250,250,250,0.85)"
                      : "rgba(161,161,170,0.55)",
                }}
              />
              {isHovered && (
                <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-10 pointer-events-none">
                  <div className="bg-zinc-900 border border-white/10 rounded px-2 py-1 text-[10px] whitespace-nowrap">
                    <span className="text-zinc-400">{point.quarter}</span>{" "}
                    <span className="text-zinc-100">{point.burn_display}</span>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function CashTrajectoryChart({
  data,
}: {
  data: CashHistoryPoint[];
}) {
  if (data.length < 3) return null;

  const latest = data[data.length - 1];

  // XAxis ticks: first, middle, last
  const tickIndices = [0, Math.floor(data.length / 2), data.length - 1];
  const ticks = [
    ...new Set(
      tickIndices.map((i) => data[i]?.quarter).filter(Boolean)
    ),
  ];

  return (
    <div>
      <div className="flex items-baseline justify-between mb-4">
        <p className="text-xs uppercase tracking-wider text-zinc-500">
          Cash Trajectory
        </p>
        <p className="text-xs text-zinc-600">{data.length} quarters</p>
      </div>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={data}
            margin={{ top: 8, right: 8, left: 0, bottom: 0 }}
          >
            <defs>
              <linearGradient
                id="cashGradient"
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
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
          </AreaChart>
        </ResponsiveContainer>
      </div>

      <BurnStrip data={data} />
    </div>
  );
}
