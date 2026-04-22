"use client";

import { useEffect, useState } from "react";
import { api, type Document } from "@/lib/api";

export function DocumentsTab({ ticker }: { ticker: string }) {
  const [docs, setDocs] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.documents({ ticker })
      .then(setDocs)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) {
    return (
      <div className="animate-pulse space-y-3">
        {[1, 2, 3, 4, 5].map((i) => (
          <div key={i} className="h-10 bg-zinc-800/30 rounded" />
        ))}
      </div>
    );
  }

  if (docs.length === 0) {
    return <p className="text-sm text-zinc-500">No documents yet.</p>;
  }

  return (
    <div className="overflow-x-auto -mx-2">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-zinc-500 border-b border-white/[0.06]">
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider">Title</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider">Date</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider">Status</th>
            <th className="px-3 py-2.5 font-medium text-xs uppercase tracking-wider w-12" />
          </tr>
        </thead>
        <tbody className="divide-y divide-white/[0.04]">
          {docs.map((d) => (
            <tr key={d.document_id} className="hover:bg-white/[0.02]">
              <td className="px-3 py-2.5 text-zinc-300 text-xs max-w-md truncate">
                {d.header || "Untitled"}
              </td>
              <td className="px-3 py-2.5 text-xs text-zinc-500 whitespace-nowrap">
                {d.announcement_date || ""}
              </td>
              <td className="px-3 py-2.5">
                <span
                  className={`text-xs ${
                    d.parse_status === "parsed"
                      ? "text-emerald-400"
                      : d.parse_status === "failed"
                        ? "text-red-400"
                        : "text-zinc-500"
                  }`}
                >
                  {d.parse_status}
                </span>
              </td>
              <td className="px-3 py-2.5">
                {d.url && !d.url.startsWith("upload://") && (
                  <a
                    href={d.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-zinc-400 hover:text-zinc-200 transition-colors"
                  >
                    PDF
                  </a>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
