"use client";

import { useEffect, useState } from "react";
import { api, type FinancialsResponse } from "@/lib/api";

function fmtShares(val: number | null | undefined) {
  if (val == null) return "";
  if (val >= 1e9) return `${(val / 1e9).toFixed(2)}B`;
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
  if (val >= 1e3) return `${(val / 1e3).toFixed(0)}K`;
  return val.toLocaleString();
}

export function CapitalTab({ ticker }: { ticker: string }) {
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
        {[1, 2, 3].map((i) => (
          <div key={i} className="h-10 bg-zinc-800/30 rounded" />
        ))}
      </div>
    );
  }

  // Filter to rows that have share data
  const rows = (data?.history ?? []).filter((f) => f.shares_basic != null);

  if (rows.length === 0) {
    return <p className="text-sm text-zinc-500">No capital structure data yet.</p>;
  }

  return (
    <div className="overflow-x-auto -mx-2">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-zinc-500 border-b border-white/[0.06]">
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider">Date</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Shares</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Options</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Perf Rights</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider text-right">Convertibles</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/[0.04]">
          {rows.map((f) => (
            <tr key={f.financial_id} className="hover:bg-white/[0.02]">
              <td className="px-3 py-2.5 text-xs text-zinc-400">{f.effective_date}</td>
              <td className="px-3 py-2.5 text-right text-zinc-200">{fmtShares(f.shares_basic)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtShares(f.options_outstanding)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtShares(f.perf_rights_outstanding)}</td>
              <td className="px-3 py-2.5 text-right text-zinc-400">{fmtShares(f.convertibles_face_value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
