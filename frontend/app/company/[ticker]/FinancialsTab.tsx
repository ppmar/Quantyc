"use client";

import { useEffect, useState } from "react";
import { api, type FinancialsResponse } from "@/lib/api";

function fmtAud(val: number | null | undefined) {
  if (val == null) return "";
  const sign = val < 0 ? "-" : "";
  const v = Math.abs(val);
  if (v >= 1e9) return `${sign}A$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `${sign}A$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${sign}A$${(v / 1e3).toFixed(0)}K`;
  return `${sign}A$${v.toFixed(0)}`;
}

function fmtShares(val: number | null | undefined) {
  if (val == null) return "";
  if (val >= 1e9) return `${(val / 1e9).toFixed(2)}B`;
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
  if (val >= 1e3) return `${(val / 1e3).toFixed(0)}K`;
  return val.toLocaleString();
}

export function FinancialsTab({ ticker }: { ticker: string }) {
  const [data, setData] = useState<FinancialsResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.financials(ticker)
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) {
    return (
      <div className="animate-pulse space-y-3">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="h-10 bg-zinc-800/30 rounded" />
        ))}
      </div>
    );
  }

  const history = data?.history ?? [];

  if (history.length === 0) {
    return <p className="text-sm text-zinc-500">No financial data yet.</p>;
  }

  return (
    <div className="overflow-x-auto -mx-2">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-zinc-500 border-b border-white/[0.06]">
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider">Date</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Cash</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Debt</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Burn / Qtr</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Shares</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Options</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/[0.04]">
          {history.map((f) => (
            <tr key={f.financial_id} className="hover:bg-white/[0.02]">
              <td className="px-3 py-2.5 text-xs text-zinc-400">{f.effective_date}</td>
              <td className="px-3 py-2.5 text-right text-zinc-200">{fmtAud(f.cash)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtAud(f.debt)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtAud(f.quarterly_opex_burn)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtShares(f.shares_basic)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtShares(f.options_outstanding)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
