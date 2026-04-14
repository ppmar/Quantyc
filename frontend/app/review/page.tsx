"use client";

import { useEffect, useState } from "react";
import { api, type ReviewItem } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { AlertTriangle } from "lucide-react";
import Link from "next/link";

function fmtAud(val: number | null | undefined) {
  if (val == null) return "-";
  if (Math.abs(val) >= 1e9) return `A$${(val / 1e9).toFixed(2)}B`;
  if (Math.abs(val) >= 1e6) return `A$${(val / 1e6).toFixed(2)}M`;
  if (Math.abs(val) >= 1e3) return `A$${(val / 1e3).toFixed(1)}K`;
  return `A$${val.toFixed(2)}`;
}

function fmtShares(val: number | null | undefined) {
  if (val == null) return "-";
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
      <div className="flex items-center justify-center h-64">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Review Queue</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {items.length} financial records flagged for review
        </p>
      </div>

      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-muted-foreground">
                  <th className="px-4 py-3 font-medium">Ticker</th>
                  <th className="px-4 py-3 font-medium">Date</th>
                  <th className="px-4 py-3 font-medium text-right">Shares FD</th>
                  <th className="px-4 py-3 font-medium text-right">Cash</th>
                  <th className="px-4 py-3 font-medium text-right">Debt</th>
                  <th className="px-4 py-3 font-medium text-right">Opex/Qtr</th>
                  <th className="px-4 py-3 font-medium">Method</th>
                  <th className="px-4 py-3 font-medium">Reason</th>
                  <th className="px-4 py-3 font-medium">Source</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/50">
                {items.length === 0 ? (
                  <tr>
                    <td colSpan={9} className="px-4 py-12 text-center text-muted-foreground">
                      No items need review
                    </td>
                  </tr>
                ) : (
                  items.map((item) => (
                    <tr key={item.financial_id} className="hover:bg-muted/30">
                      <td className="px-4 py-2.5">
                        <Link
                          href={`/company/${item.ticker}`}
                          className="font-mono font-semibold text-primary hover:underline"
                        >
                          {item.ticker}
                        </Link>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs">{item.effective_date}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{fmtShares(item.shares_fd)}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{fmtAud(item.cash)}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{fmtAud(item.debt)}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{fmtAud(item.quarterly_opex_burn)}</td>
                      <td className="px-4 py-2.5">
                        <Badge variant="secondary" className="text-[10px]">{item.extraction_method}</Badge>
                      </td>
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-1 text-xs text-destructive">
                          <AlertTriangle className="h-3 w-3" />
                          <span className="max-w-[200px] truncate" title={item.review_reason || ""}>
                            {item.review_reason || "flagged"}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-2.5">
                        {item.url && !item.url.startsWith("upload://") ? (
                          <a
                            href={item.url}
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
