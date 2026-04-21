"use client";

import { useEffect, useState } from "react";
import { api, type Stats, type Company, type ReviewItem } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Building2,
  FileText,
  CheckCircle2,
  Landmark,
  AlertTriangle,
  Clock,
} from "lucide-react";
import { IngestPanel } from "@/components/ingest-panel";
import { PipelineProgress } from "@/components/pipeline-progress";
import Link from "next/link";

export default function DashboardPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [reviewItems, setReviewItems] = useState<ReviewItem[]>([]);

  const refreshData = () => {
    api.stats().then(setStats).catch(() => {});
    api.companies().then(setCompanies).catch(() => {});
    api.review().then(setReviewItems).catch(() => {});
  };

  useEffect(() => {
    refreshData();
  }, []);

  if (!stats) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold tracking-tight">Dashboard</h1>

      <IngestPanel />
      <PipelineProgress onComplete={refreshData} />

      {/* Stats row */}
      <div className="grid grid-cols-3 gap-3 sm:grid-cols-6">
        {[
          { label: "Companies", value: stats.companies, icon: Building2 },
          { label: "Documents", value: stats.documents, icon: FileText },
          { label: "Parsed", value: stats.docs_parsed, icon: CheckCircle2 },
          { label: "Pending", value: stats.docs_pending + stats.docs_classified, icon: Clock },
          { label: "Financials", value: stats.financials, icon: Landmark },
          { label: "Review", value: stats.needs_review, icon: AlertTriangle },
        ].map(({ label, value, icon: Icon }) => (
          <Card key={label}>
            <CardContent className="p-3">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
                  <p className="text-xl font-bold font-mono">{value}</p>
                </div>
                <Icon className="h-4 w-4 text-muted-foreground/40" />
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Review alerts */}
      {reviewItems.length > 0 && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-destructive flex items-center gap-1.5">
              <AlertTriangle className="h-4 w-4" />
              Needs Review ({reviewItems.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-1.5">
              {reviewItems.slice(0, 8).map((item) => (
                <div key={item.financial_id} className="flex items-center gap-3 text-sm">
                  <Badge variant="secondary" className="font-mono text-xs shrink-0">
                    {item.ticker}
                  </Badge>
                  <span className="text-xs text-muted-foreground truncate">
                    {item.effective_date} — {item.review_reason || "flagged"}
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Companies table */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Companies</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-2.5 font-medium">Ticker</th>
                  <th className="px-4 py-2.5 font-medium">Name</th>
                  <th className="px-4 py-2.5 font-medium text-right">Docs</th>
                  <th className="px-4 py-2.5 font-medium text-right">Parsed</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/50">
                {companies.map((c) => (
                  <tr key={c.ticker} className="hover:bg-muted/30">
                    <td className="px-4 py-2">
                      <Link
                        href={`/company/${c.ticker}`}
                        className="font-bold font-mono text-primary hover:underline"
                      >
                        {c.ticker}
                      </Link>
                    </td>
                    <td className="px-4 py-2 text-muted-foreground text-xs">
                      {c.name || "-"}
                    </td>
                    <td className="px-4 py-2 text-right font-mono text-xs">
                      {c.doc_count}
                    </td>
                    <td className="px-4 py-2 text-right font-mono text-xs">
                      {c.parsed_count}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
