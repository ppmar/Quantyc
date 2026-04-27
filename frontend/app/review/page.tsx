"use client";

import { useEffect, useState } from "react";
import { api, type ReviewItem } from "@/lib/api";
import Link from "next/link";

function fmtAud(val: number | null | undefined) {
  if (val == null) return "\u2014";
  const sign = val < 0 ? "-" : "";
  const v = Math.abs(val);
  if (v >= 1e9) return `${sign}A$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `${sign}A$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${sign}A$${(v / 1e3).toFixed(0)}K`;
  return `${sign}A$${v.toFixed(0)}`;
}

function fmtShares(val: number | null | undefined) {
  if (val == null) return "\u2014";
  if (val >= 1e9) return `${(val / 1e9).toFixed(2)}B`;
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
  return val.toLocaleString();
}

export default function ReviewPage() {
  const [items, setItems] = useState<ReviewItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .review()
      .then(setItems)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="animate-pulse space-y-3">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="h-8 bg-zinc-800/20 rounded" />
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <p className="text-xs uppercase tracking-wider text-zinc-500">
          Review Queue
        </p>
        <p className="text-xs text-zinc-600">{items.length} flagged</p>
      </div>

      {items.length === 0 ? (
        <p className="text-[13px] text-zinc-600 py-8">
          No records flagged for review.
        </p>
      ) : (
        <div className="overflow-x-auto -mx-2">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left border-b border-border">
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Ticker</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Date</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">Cash</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">Burn/Qtr</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Reason</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Source</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {items.map((item) => (
                <tr key={item.financial_id} className="hover:bg-white/[0.02] transition-colors">
                  <td className="px-3 py-2">
                    <Link
                      href={`/company/${item.ticker}`}
                      className="font-mono text-[13px] font-medium text-amber hover:text-amber/80 transition-colors"
                    >
                      {item.ticker}
                    </Link>
                  </td>
                  <td className="px-3 py-2 font-mono text-[12px] text-zinc-600">
                    {item.effective_date}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-[13px] text-zinc-400">
                    {fmtAud(item.cash)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-[13px] text-zinc-500">
                    {fmtAud(item.quarterly_opex_burn)}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className="text-[12px] text-red-400/80 max-w-[200px] truncate block"
                      title={item.review_reason || ""}
                    >
                      {item.review_reason || "flagged"}
                    </span>
                  </td>
                  <td className="px-3 py-2">
                    {item.url && !item.url.startsWith("upload://") ? (
                      <a
                        href={item.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-[12px] text-zinc-500 hover:text-zinc-300 transition-colors"
                      >
                        PDF
                      </a>
                    ) : (
                      <span className="text-[12px] text-zinc-700">{"\u2014"}</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
