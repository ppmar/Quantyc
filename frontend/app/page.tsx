"use client";

import { useEffect, useState } from "react";
import { api, type Stats, type Company } from "@/lib/api";
import { IngestPanel } from "@/components/ingest-panel";
import { PipelineProgress } from "@/components/pipeline-progress";
import { PageHeader } from "@/components/page-header";
import Link from "next/link";

function StatCard({
  label,
  value,
  hint,
  tone = "default",
  delay = 0,
}: {
  label: string;
  value: number;
  hint?: string;
  tone?: "default" | "accent" | "warn";
  delay?: number;
}) {
  const valueCls =
    tone === "warn" && value > 0
      ? "text-amber"
      : tone === "accent"
        ? "text-amber"
        : "text-zinc-100";
  return (
    <div
      className="q-card q-card-hero animate-fade-up px-5 py-4"
      style={{ animationDelay: `${delay}ms` }}
    >
      <p className="q-label">{label}</p>
      <p className={`mt-2 font-mono text-[26px] leading-none ${valueCls}`}>
        {value.toLocaleString()}
      </p>
      {hint && <p className="mt-1.5 text-[11px] text-zinc-600">{hint}</p>}
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
      <div className="space-y-6">
        <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="h-24 q-shimmer" />
          ))}
        </div>
        <div className="h-64 q-shimmer" />
      </div>
    );
  }

  const parsedPct =
    stats.documents > 0
      ? Math.round((stats.docs_parsed / stats.documents) * 100)
      : 0;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Overview"
        subtitle="Ingestion and coverage across the tracked universe"
      />

      {/* Stat cards */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <StatCard label="Companies" value={stats.companies} delay={0} />
        <StatCard
          label="Documents"
          value={stats.documents}
          delay={40}
        />
        <StatCard
          label="Parsed"
          value={stats.docs_parsed}
          hint={`${parsedPct}% of corpus`}
          tone="accent"
          delay={80}
        />
        <StatCard
          label="Pending"
          value={stats.docs_pending + stats.docs_classified}
          delay={120}
        />
        <StatCard
          label="Failed"
          value={stats.docs_failed}
          tone="warn"
          delay={160}
        />
      </div>

      <PipelineProgress onComplete={refreshData} />
      <IngestPanel />

      {/* Companies table */}
      <div className="q-card animate-fade-up overflow-hidden" style={{ animationDelay: "200ms" }}>
        <div className="flex items-baseline justify-between border-b border-border px-5 py-3.5">
          <p className="q-label">Tracked companies</p>
          <p className="text-[11px] font-mono text-zinc-600">
            {companies.length} tickers
          </p>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left border-b border-border">
                <th className="px-5 py-2.5 font-medium text-[11px] uppercase tracking-wider text-zinc-500">
                  Ticker
                </th>
                <th className="px-3 py-2.5 font-medium text-[11px] uppercase tracking-wider text-zinc-500">
                  Name
                </th>
                <th className="px-3 py-2.5 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">
                  Docs
                </th>
                <th className="px-5 py-2.5 font-medium text-[11px] uppercase tracking-wider text-zinc-500 text-right">
                  Parsed
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {companies.map((c) => (
                <tr
                  key={c.ticker}
                  className="group hover:bg-white/[0.025] transition-colors"
                >
                  <td className="px-5 py-2 border-l-2 border-transparent group-hover:border-amber/50 transition-colors">
                    <Link
                      href={`/company/${c.ticker}`}
                      className="font-mono text-[13px] font-medium text-amber hover:text-amber/80 transition-colors"
                    >
                      {c.ticker}
                    </Link>
                  </td>
                  <td className="px-3 py-2 text-zinc-400 text-[13px]">
                    {c.name || "—"}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-[13px] text-zinc-400">
                    {c.doc_count}
                  </td>
                  <td className="px-5 py-2 text-right font-mono text-[13px] text-zinc-500">
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
