"use client";

import { useEffect, useState } from "react";
import { api, type Document } from "@/lib/api";
import Link from "next/link";

const STATUS_COLOR: Record<string, string> = {
  parsed: "text-emerald-400",
  classified: "text-blue-400",
  pending: "text-amber",
  failed: "text-red-400",
  skipped: "text-zinc-600",
};

export default function DocumentsPage() {
  const [docs, setDocs] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [typeFilter, setTypeFilter] = useState<string>("");

  useEffect(() => {
    setLoading(true);
    api
      .documents({
        status: statusFilter || undefined,
        type: typeFilter || undefined,
      })
      .then(setDocs)
      .finally(() => setLoading(false));
  }, [statusFilter, typeFilter]);

  const statuses = [...new Set(docs.map((d) => d.parse_status))].sort();
  const types = [...new Set(docs.map((d) => d.doc_type))].sort();

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <p className="text-xs uppercase tracking-wider text-zinc-500">
          Documents
        </p>
        <p className="text-xs text-zinc-600">{docs.length} in pipeline</p>
      </div>

      <div className="flex gap-2">
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600"
        >
          <option value="">All statuses</option>
          {statuses.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <select
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600"
        >
          <option value="">All types</option>
          {types.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      <div className="overflow-x-auto -mx-2">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left border-b border-border">
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Ticker</th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Type</th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Header</th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Status</th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Date</th>
              <th className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500">Link</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {loading ? (
              <tr>
                <td colSpan={6} className="px-3 py-8 text-center text-zinc-600 text-[13px]">
                  Loading...
                </td>
              </tr>
            ) : docs.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-3 py-8 text-center text-zinc-600 text-[13px]">
                  No documents found
                </td>
              </tr>
            ) : (
              docs.map((doc) => (
                <tr
                  key={doc.document_id}
                  className="hover:bg-white/[0.02] transition-colors"
                >
                  <td className="px-3 py-2">
                    <Link
                      href={`/company/${doc.ticker}`}
                      className="font-mono text-[13px] font-medium text-amber hover:text-amber/80 transition-colors"
                    >
                      {doc.ticker}
                    </Link>
                  </td>
                  <td className="px-3 py-2 text-[12px] text-zinc-500 font-mono">
                    {doc.doc_type}
                  </td>
                  <td className="px-3 py-2 text-[13px] text-zinc-500 max-w-xs truncate">
                    {doc.header || "\u2014"}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className={`text-[12px] font-mono ${STATUS_COLOR[doc.parse_status] || "text-zinc-500"}`}
                      title={doc.parse_error || undefined}
                    >
                      {doc.parse_status}
                    </span>
                    {doc.parse_status === "failed" && doc.parse_error && (
                      <p
                        className="text-[10px] text-red-400/60 font-mono mt-0.5 truncate max-w-[180px]"
                        title={doc.parse_error}
                      >
                        {doc.parse_error}
                      </p>
                    )}
                  </td>
                  <td className="px-3 py-2 font-mono text-[12px] text-zinc-600">
                    {doc.announcement_date || "\u2014"}
                  </td>
                  <td className="px-3 py-2">
                    {doc.url && !doc.url.startsWith("upload://") ? (
                      <a
                        href={doc.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-[12px] text-zinc-500 hover:text-zinc-300 transition-colors"
                      >
                        PDF
                      </a>
                    ) : (
                      <span className="text-[12px] text-zinc-700">\u2014</span>
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
