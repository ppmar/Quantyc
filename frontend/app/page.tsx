"use client";

import { useEffect, useState } from "react";
import { api, type Stats, type Company } from "@/lib/api";
import { IngestPanel } from "@/components/ingest-panel";
import { PipelineProgress } from "@/components/pipeline-progress";
import Link from "next/link";

function StatValue({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <p className="text-[10px] uppercase tracking-wider text-zinc-600">{label}</p>
      <p className="text-xl font-medium text-zinc-100 mt-0.5">{value}</p>
    </div>
  );
}

export default function DashboardPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [companies, setCompanies] = useState<Company[]>([]);

  const refreshData = () => {
    api.stats().then(setStats).catch(() => {});
    api.companies().then(setCompanies).catch(() => {});
  };

  useEffect(() => {
    refreshData();
  }, []);

  if (!stats) {
    return (
      <div className="animate-pulse space-y-8">
        <div className="flex gap-12">
          {[1, 2, 3, 4].map((i) => (
            <div key={i}>
              <div className="h-3 w-16 bg-zinc-800/40 rounded mb-2" />
              <div className="h-6 w-12 bg-zinc-800 rounded" />
            </div>
          ))}
        </div>
        <div className="h-px bg-border" />
        {[1, 2, 3, 4, 5].map((i) => (
          <div key={i} className="h-8 bg-zinc-800/20 rounded" />
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Stats strip */}
      <div className="flex gap-12 flex-wrap">
        <StatValue label="Companies" value={stats.companies} />
        <StatValue label="Documents" value={stats.documents} />
        <StatValue label="Parsed" value={stats.docs_parsed} />
        <StatValue label="Pending" value={stats.docs_pending + stats.docs_classified} />
        <StatValue label="Failed" value={stats.docs_failed} />
      </div>

      <PipelineProgress onComplete={refreshData} />
      <IngestPanel />

      <div className="border-t border-border" />

      {/* Companies table */}
      <div>
        <div className="flex items-baseline justify-between mb-4">
          <p className="text-xs uppercase tracking-wider text-zinc-500">
            Companies
          </p>
          <p className="text-xs text-zinc-600">{companies.length} tracked</p>
        </div>

        <div className="overflow-x-auto -mx-2">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left border-b border-border">
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Ticker</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Name</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">Docs</th>
                <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">Parsed</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {companies.map((c) => (
                <tr key={c.ticker} className="hover:bg-white/[0.02] transition-colors">
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
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
