"use client";

import { useEffect, useState } from "react";
import { api, type Document } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import Link from "next/link";

const STATUS_STYLE: Record<string, string> = {
  parsed: "bg-green-500/10 text-green-400 border-green-500/30",
  classified: "bg-blue-500/10 text-blue-400 border-blue-500/30",
  pending: "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  failed: "bg-red-500/10 text-red-400 border-red-500/30",
  skipped: "bg-gray-500/10 text-gray-400 border-gray-500/30",
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

  const statuses = [...new Set(docs.map((d) => d.parse_status))];
  const types = [...new Set(docs.map((d) => d.doc_type))];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Documents</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {docs.length} documents in pipeline
        </p>
      </div>

      <div className="flex gap-2 flex-wrap">
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="rounded-md border border-border bg-card px-3 py-1.5 text-sm text-foreground"
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
          className="rounded-md border border-border bg-card px-3 py-1.5 text-sm text-foreground"
        >
          <option value="">All types</option>
          {types.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-muted-foreground">
                  <th className="px-4 py-3 font-medium">Ticker</th>
                  <th className="px-4 py-3 font-medium">Type</th>
                  <th className="px-4 py-3 font-medium">Header</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Date</th>
                  <th className="px-4 py-3 font-medium">Link</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/50">
                {loading ? (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                      Loading...
                    </td>
                  </tr>
                ) : docs.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                      No documents found
                    </td>
                  </tr>
                ) : (
                  docs.map((doc) => (
                    <tr
                      key={doc.document_id}
                      className="hover:bg-muted/30 transition-colors"
                    >
                      <td className="px-4 py-2.5">
                        <Link
                          href={`/company/${doc.ticker}`}
                          className="font-semibold font-mono text-primary hover:underline"
                        >
                          {doc.ticker}
                        </Link>
                      </td>
                      <td className="px-4 py-2.5">
                        <Badge variant="secondary" className="text-xs">
                          {doc.doc_type}
                        </Badge>
                      </td>
                      <td className="px-4 py-2.5 text-muted-foreground max-w-xs truncate">
                        {doc.header || "-"}
                      </td>
                      <td className="px-4 py-2.5">
                        <span
                          className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium border ${
                            STATUS_STYLE[doc.parse_status] || ""
                          }`}
                        >
                          {doc.parse_status}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-muted-foreground font-mono text-xs">
                        {doc.announcement_date || "-"}
                      </td>
                      <td className="px-4 py-2.5">
                        {doc.url && !doc.url.startsWith("upload://") ? (
                          <a
                            href={doc.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-xs text-primary hover:underline"
                          >
                            PDF
                          </a>
                        ) : (
                          <span className="text-xs text-muted-foreground">-</span>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
