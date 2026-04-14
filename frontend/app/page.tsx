"use client";

import { useEffect, useState } from "react";
import { api, type Stats, type Company, type ReviewItem } from "@/lib/api";
import { StatCard } from "@/components/stat-card";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Building2,
  FileText,
  CheckCircle2,
  Landmark,
  AlertTriangle,
  Upload,
  ClipboardList,
  Clock,
} from "lucide-react";
import { UploadZone } from "@/components/upload-zone";
import { IngestPanel } from "@/components/ingest-panel";
import { PipelineProgress } from "@/components/pipeline-progress";
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import Link from "next/link";

const STATUS_COLORS: Record<string, string> = {
  parsed: "#22c55e",
  classified: "#3b82f6",
  pending: "#eab308",
  failed: "#ef4444",
};

export default function DashboardPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [reviewItems, setReviewItems] = useState<ReviewItem[]>([]);
  const [showUpload, setShowUpload] = useState(false);

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

  const statusData = [
    { name: "Parsed", value: stats.docs_parsed, color: STATUS_COLORS.parsed },
    { name: "Classified", value: stats.docs_classified, color: STATUS_COLORS.classified },
    { name: "Pending", value: stats.docs_pending, color: STATUS_COLORS.pending },
    { name: "Failed", value: stats.docs_failed, color: STATUS_COLORS.failed },
  ].filter((d) => d.value > 0);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Dashboard</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Pipeline overview and data coverage
        </p>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm font-medium flex items-center gap-1.5">
              <Upload className="h-4 w-4" />
              Upload Documents
            </CardTitle>
            <button
              onClick={() => setShowUpload(!showUpload)}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              {showUpload ? "Hide" : "Show"}
            </button>
          </div>
        </CardHeader>
        {showUpload && (
          <CardContent>
            <UploadZone onComplete={refreshData} />
          </CardContent>
        )}
      </Card>

      <IngestPanel />

      <PipelineProgress onComplete={refreshData} />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <StatCard label="Companies" value={stats.companies} icon={Building2} />
        <StatCard label="Documents" value={stats.documents} icon={FileText} />
        <StatCard label="Parsed" value={stats.docs_parsed} icon={CheckCircle2} />
        <StatCard label="Classified" value={stats.docs_classified} icon={ClipboardList} />
        <StatCard label="Financials" value={stats.financials} icon={Landmark} />
        <StatCard label="Review" value={stats.needs_review} icon={Clock} />
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Parse Status</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-[220px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={statusData}
                  cx="50%"
                  cy="50%"
                  innerRadius={55}
                  outerRadius={85}
                  paddingAngle={3}
                  dataKey="value"
                  stroke="none"
                >
                  {statusData.map((entry, i) => (
                    <Cell key={i} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    background: "#1a1a2e",
                    border: "1px solid rgba(255,255,255,0.1)",
                    borderRadius: "8px",
                    fontSize: "13px",
                    color: "#e5e5e5",
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="flex justify-center gap-4 mt-2">
            {statusData.map((d) => (
              <div key={d.name} className="flex items-center gap-1.5 text-xs">
                <div
                  className="h-2.5 w-2.5 rounded-full"
                  style={{ background: d.color }}
                />
                <span className="text-muted-foreground">
                  {d.name} ({d.value})
                </span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {reviewItems.length > 0 && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-destructive flex items-center gap-1.5">
              <AlertTriangle className="h-4 w-4" />
              Needs Review ({reviewItems.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {reviewItems.slice(0, 10).map((item) => (
                <div key={item.financial_id} className="flex items-start gap-3 text-sm">
                  <Badge
                    variant="secondary"
                    className="font-mono text-xs shrink-0"
                  >
                    {item.ticker}
                  </Badge>
                  <span className="text-muted-foreground">
                    {item.effective_date} — {item.review_reason || "flagged for review"}
                  </span>
                  <Badge variant="outline" className="text-[10px] ml-auto shrink-0">
                    {item.confidence}
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">Companies</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-muted-foreground border-b border-border">
                  <th className="pb-2 pr-4 font-medium">Ticker</th>
                  <th className="pb-2 pr-4 font-medium">Name</th>
                  <th className="pb-2 pr-4 font-medium">Currency</th>
                  <th className="pb-2 pr-4 font-medium text-right">Docs</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/50">
                {companies.map((c) => (
                  <tr
                    key={c.ticker}
                    className="hover:bg-muted/30 transition-colors"
                  >
                    <td className="py-2.5 pr-4">
                      <Link
                        href={`/company/${c.ticker}`}
                        className="font-semibold font-mono text-primary hover:underline"
                      >
                        {c.ticker}
                      </Link>
                    </td>
                    <td className="py-2.5 pr-4 text-muted-foreground">
                      {c.name || "-"}
                    </td>
                    <td className="py-2.5 pr-4 text-muted-foreground">
                      {c.reporting_currency}
                    </td>
                    <td className="py-2.5 pr-4 text-right tabular-nums font-mono">
                      {c.parsed_count}/{c.doc_count}
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
