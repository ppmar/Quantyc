"use client";

import { useCallback, useEffect, useState } from "react";
import { api, type IngestHealth } from "@/lib/api";

function Card({ label, value, accent }: { label: string; value: string | number; accent?: string }) {
  return (
    <div className="rounded-sm border border-border p-4">
      <p className="text-[11px] uppercase tracking-wider text-zinc-500">{label}</p>
      <p className={`mt-1 text-2xl font-mono ${accent ?? "text-zinc-100"}`}>{value}</p>
    </div>
  );
}

export default function HealthPage() {
  const [data, setData] = useState<IngestHealth | null>(null);
  const [loading, setLoading] = useState(true);
  const [retrying, setRetrying] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    api.healthIngest().then(setData).finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const onRetry = async () => {
    setRetrying(true);
    setMsg(null);
    try {
      const r = await api.retryFailed();
      setMsg(`Re-queued ${r.reset} docs — orchestrator ${r.orchestrate}.`);
      setTimeout(load, 1500);
    } finally {
      setRetrying(false);
    }
  };

  if (loading || !data) {
    return <p className="text-[13px] text-zinc-600">Loading…</p>;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <p className="text-xs uppercase tracking-wider text-zinc-500">Ingest Health</p>
        <button
          onClick={onRetry}
          disabled={retrying}
          className="h-8 rounded-sm border border-border px-3 text-[12px] text-amber hover:border-zinc-600 disabled:opacity-40"
        >
          {retrying ? "Retrying…" : "Retry transient failures now"}
        </button>
      </div>

      {msg && <p className="text-[12px] text-emerald-400">{msg}</p>}

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Card label="Recoverable gap" value={data.study_coverage.recoverable_gap} accent="text-amber" />
        <Card label="Companies w/ study doc" value={data.study_coverage.companies_with_study_doc} />
        <Card label="Companies w/ parsed study" value={data.study_coverage.companies_with_parsed_study} accent="text-emerald-400" />
        <Card label="Retry queue (due now)" value={`${data.retry_queue.scheduled} (${data.retry_queue.due_now})`} />
        <Card label="Failed — transient" value={data.failures_by_class.transient} accent="text-amber" />
        <Card label="Failed — permanent" value={data.failures_by_class.permanent} accent="text-red-400" />
        <Card label="Failed — unclassified (legacy)" value={data.failures_by_class.unclassified} accent="text-zinc-400" />
        <Card label="Parsed" value={data.totals.parsed} accent="text-emerald-400" />
        <Card label="Documents" value={data.totals.documents} />
      </div>

      <div>
        <p className="mb-2 text-[11px] uppercase tracking-wider text-zinc-500">
          Error buckets (failed + retry-scheduled)
        </p>
        <div className="overflow-x-auto -mx-2">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left">
                <th className="px-3 py-2 text-[11px] uppercase tracking-wider text-zinc-500">Reason</th>
                <th className="px-3 py-2 text-[11px] uppercase tracking-wider text-zinc-500">Doc type</th>
                <th className="px-3 py-2 text-right text-[11px] uppercase tracking-wider text-zinc-500">Count</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {data.error_buckets.map((b, i) => (
                <tr key={i} className="hover:bg-white/[0.02]">
                  <td className="px-3 py-2 font-mono text-[12px] text-zinc-400 max-w-md truncate" title={b.reason}>{b.reason}</td>
                  <td className="px-3 py-2 font-mono text-[12px] text-zinc-500">{b.doc_type}</td>
                  <td className="px-3 py-2 text-right font-mono text-[13px] text-zinc-300">{b.count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
