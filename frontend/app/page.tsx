"use client";

import { useEffect, useState } from "react";
import { api, type Stats, type Company, type RedFlag } from "@/lib/api";
import { StatCard } from "@/components/stat-card";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Building2,
  FileText,
  CheckCircle2,
  Layers,
  FlaskConical,
  Crosshair,
  AlertTriangle,
  Upload,
} from "lucide-react";
import { UploadZone } from "@/components/upload-zone";
import { PipelineProgress } from "@/components/pipeline-progress";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import Link from "next/link";

const STATUS_COLORS: Record<string, string> = {
  done: "#22c55e",
  pending: "#eab308",
  failed: "#ef4444",
  needs_review: "#3b82f6",
};

export default function DashboardPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [review, setReview] = useState<{ red_flags: RedFlag[] }>({
    red_flags: [],
  });

  const [showUpload, setShowUpload] = useState(false);

  const refreshData = () => {
    api.stats().then(setStats).catch(() => {});
    api.companies().then(setCompanies).catch(() => {});
    api
      .review()
      .then((d) => setReview({ red_flags: d.red_flags }))
      .catch(() => {});
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
    { name: "Done", value: stats.docs_done, color: STATUS_COLORS.done },
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

      <PipelineProgress onComplete={refreshData} />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <StatCard label="Companies" value={stats.companies} icon={Building2} />
        <StatCard label="Documents" value={stats.documents} icon={FileText} />
        <StatCard label="Parsed" value={stats.docs_done} icon={CheckCircle2} />
        <StatCard label="Resources" value={stats.resources} icon={Layers} />
        <StatCard label="Studies" value={stats.studies} icon={FlaskConical} />
        <StatCard label="Drill Holes" value={stats.drill_holes} icon={Crosshair} />
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
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

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Companies by Stage
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[250px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={(() => {
                    const counts: Record<string, number> = {};
                    companies.forEach((c) => {
                      const s = c.stage || "unknown";
                      counts[s] = (counts[s] || 0) + 1;
                    });
                    return Object.entries(counts).map(([stage, count]) => ({
                      stage,
                      count,
                    }));
                  })()}
                  margin={{ top: 8, right: 8, bottom: 0, left: -16 }}
                >
                  <XAxis
                    dataKey="stage"
                    tick={{
                      fill: "#a3a3a3",
                      fontSize: 12,
                    }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{
                      fill: "#a3a3a3",
                      fontSize: 12,
                    }}
                    axisLine={false}
                    tickLine={false}
                    allowDecimals={false}
                  />
                  <Tooltip
                    contentStyle={{
                      background: "#1a1a2e",
                      border: "1px solid rgba(255,255,255,0.1)",
                      borderRadius: "8px",
                      fontSize: "13px",
                      color: "#e5e5e5",
                    }}
                  />
                  <Bar
                    dataKey="count"
                    fill="#eab308"
                    radius={[4, 4, 0, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {review.red_flags.length > 0 && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-destructive flex items-center gap-1.5">
              <AlertTriangle className="h-4 w-4" />
              Red Flags ({review.red_flags.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {review.red_flags.map((flag, i) => (
                <div key={i} className="flex items-start gap-3 text-sm">
                  <Badge
                    variant="secondary"
                    className="font-mono text-xs shrink-0"
                  >
                    {flag.ticker}
                  </Badge>
                  <span className="text-muted-foreground">
                    {flag.description}
                  </span>
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
                  <th className="pb-2 pr-4 font-medium">Commodity</th>
                  <th className="pb-2 pr-4 font-medium">Stage</th>
                  <th className="pb-2 pr-4 font-medium text-right">Docs</th>
                  <th className="pb-2 pr-4 font-medium text-right">Cash</th>
                  <th className="pb-2 font-medium text-right">Runway</th>
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
                    <td className="py-2.5 pr-4 capitalize text-muted-foreground">
                      {c.primary_commodity || "-"}
                    </td>
                    <td className="py-2.5 pr-4">
                      {c.stage ? (
                        <Badge
                          variant="outline"
                          className="text-xs capitalize"
                        >
                          {c.stage}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </td>
                    <td className="py-2.5 pr-4 text-right tabular-nums font-mono">
                      {c.parsed_count}/{c.doc_count}
                    </td>
                    <td className="py-2.5 pr-4 text-right tabular-nums font-mono text-muted-foreground">
                      {c.cash ? `A$${(c.cash / 1e6).toFixed(1)}M` : "-"}
                    </td>
                    <td className="py-2.5 text-right tabular-nums font-mono">
                      {c.runway != null ? (
                        <span
                          className={
                            c.runway < 6
                              ? "text-destructive"
                              : c.runway < 12
                              ? "text-yellow-500"
                              : "text-green-500"
                          }
                        >
                          {c.runway.toFixed(1)}mo
                        </span>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
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
