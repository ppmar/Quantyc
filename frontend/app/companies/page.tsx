"use client";

import { useEffect, useState } from "react";
import { api, type Company } from "@/lib/api";
import Link from "next/link";

export default function CompaniesPage() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  useEffect(() => {
    api
      .companies()
      .then(setCompanies)
      .finally(() => setLoading(false));
  }, []);

  const filtered = search
    ? companies.filter(
        (c) =>
          c.ticker.toLowerCase().includes(search.toLowerCase()) ||
          (c.name || "").toLowerCase().includes(search.toLowerCase())
      )
    : companies;

  if (loading) {
    return (
      <div className="animate-pulse space-y-3">
        {[1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
          <div key={i} className="h-8 bg-zinc-800/20 rounded" />
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <p className="text-xs uppercase tracking-wider text-zinc-500">
          Companies
        </p>
        <p className="text-xs text-zinc-600">{filtered.length} tracked</p>
      </div>

      <input
        type="text"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        placeholder="Search ticker or name..."
        className="w-full max-w-xs h-8 rounded-sm border border-border bg-transparent px-3 text-[13px] text-zinc-200 placeholder:text-zinc-700 focus:outline-none focus:border-zinc-600 transition-colors"
      />

      <div className="overflow-x-auto -mx-2">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left border-b border-border">
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">
                Ticker
              </th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">
                Name
              </th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">
                Docs
              </th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">
                Parsed
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {filtered.map((c) => (
              <tr
                key={c.ticker}
                className="hover:bg-white/[0.02] transition-colors"
              >
                <td className="px-3 py-2">
                  <Link
                    href={`/company/${c.ticker}`}
                    className="font-mono text-[13px] font-medium text-amber hover:text-amber/80 transition-colors"
                  >
                    {c.ticker}
                  </Link>
                </td>
                <td className="px-3 py-2 text-zinc-400 text-[13px]">
                  {c.name || "\u2014"}
                </td>
                <td className="px-3 py-2 text-right font-mono text-[13px] text-zinc-400">
                  {c.doc_count}
                </td>
                <td className="px-3 py-2 text-right font-mono text-[13px] text-zinc-500">
                  {c.parsed_count}/{c.doc_count}
                </td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={4} className="px-3 py-8 text-center text-zinc-600 text-[13px]">
                  No matches
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
