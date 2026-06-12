"use client";

import { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  ResponsiveContainer,
  Tooltip,
  Legend,
} from "recharts";
import { api, type PriceComparisonResponse } from "@/lib/api";
import type { ProjectData } from "@/types/snapshot";

const FEED_COMMODITIES = ["Au", "Ag", "Cu"];
const COMMODITY_LABEL: Record<string, string> = { Au: "Gold", Ag: "Silver", Cu: "Copper" };
const RANGES = ["6m", "1y", "3y", "5y", "max"];
const RANGE_LABEL: Record<string, string> = {
  "6m": "6M", "1y": "1Y", "3y": "3Y", "5y": "5Y", "max": "Max",
};

type IndexedPoint = { date: string; stockIdx: number; commodityIdx: number };

function rebase(series: PriceComparisonResponse["series"]): IndexedPoint[] {
  if (series.length === 0) return [];
  const baseStock = series[0].stock;
  const baseComm = series[0].commodity;
  if (!baseStock || !baseComm) return [];
  return series.map((p) => ({
    date: p.date,
    stockIdx: (p.stock / baseStock) * 100,
    commodityIdx: (p.commodity / baseComm) * 100,
  }));
}

export function ComparisonTab({
  ticker,
  projects,
}: {
  ticker: string;
  projects: ProjectData[];
}) {
  // Commodities this company has that we have a price feed for.
  const feedable = useMemo(() => {
    const set = new Set<string>();
    for (const p of projects) for (const c of p.commodities) {
      if (FEED_COMMODITIES.includes(c)) set.add(c);
    }
    return FEED_COMMODITIES.filter((c) => set.has(c));
  }, [projects]);

  const [commodity, setCommodity] = useState(feedable[0] ?? "Au");
  const [range, setRange] = useState("1y");
  const [data, setData] = useState<PriceComparisonResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .priceComparison(ticker, commodity, range)
      .then((res) => {
        if (cancelled) return;
        if (res.error) setError(res.error);
        setData(res);
      })
      .catch(() => !cancelled && setError("fetch_failed"))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [ticker, commodity, range]);

  const indexed = useMemo(() => rebase(data?.series ?? []), [data]);

  if (feedable.length === 0) {
    return <p className="text-sm text-zinc-500">No Au/Ag/Cu commodity to compare.</p>;
  }

  const tickDates = indexed.length
    ? [...new Set([0, Math.floor(indexed.length / 2), indexed.length - 1].map((i) => indexed[i]?.date).filter(Boolean))]
    : [];

  return (
    <div className="q-card q-card-hero space-y-4 p-5">
      <div className="flex items-center gap-3 flex-wrap">
        <select
          value={commodity}
          onChange={(e) => setCommodity(e.target.value)}
          className="q-control q-select"
        >
          {feedable.map((c) => (
            <option key={c} value={c}>{COMMODITY_LABEL[c] ?? c}</option>
          ))}
        </select>

        <div className="ml-auto flex gap-0.5 rounded-md border border-white/[0.06] p-0.5">
          {RANGES.map((r) => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={`h-7 rounded-[5px] px-2.5 text-[12px] transition-colors ${
                range === r
                  ? "bg-amber/15 text-amber"
                  : "text-zinc-500 hover:text-zinc-300"
              }`}
            >
              {RANGE_LABEL[r]}
            </button>
          ))}
        </div>
      </div>

      <p className="text-xs text-zinc-500">
        {ticker} share price vs {COMMODITY_LABEL[commodity] ?? commodity} spot, rebased to 100 at window start.
      </p>

      <div className="h-72">
        {loading && <p className="text-sm text-zinc-500">Loading…</p>}
        {!loading && error && <p className="text-sm text-zinc-500">No price data ({error}).</p>}
        {!loading && !error && indexed.length === 0 && (
          <p className="text-sm text-zinc-500">No price data.</p>
        )}
        {!loading && !error && indexed.length > 0 && (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={indexed} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <XAxis
                dataKey="date"
                axisLine={false}
                tickLine={false}
                tick={{ fill: "#71717a", fontSize: 11 }}
                ticks={tickDates}
              />
              <YAxis
                orientation="right"
                axisLine={false}
                tickLine={false}
                tick={{ fill: "#71717a", fontSize: 11 }}
                width={44}
                domain={["auto", "auto"]}
              />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 4, fontSize: 12 }}
                labelStyle={{ color: "#a1a1aa" }}
                formatter={(v) => Number(v).toFixed(1)}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Line
                type="monotone"
                dataKey="stockIdx"
                name={`${ticker} share`}
                stroke="#fafafa"
                strokeWidth={1.5}
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="commodityIdx"
                name={`${COMMODITY_LABEL[commodity] ?? commodity} spot`}
                stroke="#f59e0b"
                strokeWidth={1.5}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
