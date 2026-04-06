"use client";

import { useEffect, useState, use } from "react";
import { api, type CompanyDetail } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import {
  AlertTriangle,
  ArrowLeft,
  FileText,
  Layers,
  FlaskConical,
  Crosshair,
  TrendingUp,
  Landmark,
} from "lucide-react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import Link from "next/link";

function fmtAud(val: number | null | undefined) {
  if (val == null) return "-";
  if (Math.abs(val) >= 1e9) return `A$${(val / 1e9).toFixed(2)}B`;
  if (Math.abs(val) >= 1e6) return `A$${(val / 1e6).toFixed(2)}M`;
  if (Math.abs(val) >= 1e3) return `A$${(val / 1e3).toFixed(1)}K`;
  return `A$${val.toFixed(2)}`;
}

const CATEGORY_COLORS: Record<string, string> = {
  Measured: "#22c55e",
  Indicated: "#3b82f6",
  Inferred: "#f59e0b",
  Proven: "#16a34a",
  Probable: "#2563eb",
};

export default function CompanyPage({
  params,
}: {
  params: Promise<{ ticker: string }>;
}) {
  const { ticker } = use(params);
  const [data, setData] = useState<CompanyDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .company(ticker)
      .then(setData)
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  if (!data) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        Company not found
      </div>
    );
  }

  const { company, financials, projects, resources, studies, documents, drill_results, valuation } =
    data;

  const resourceChartData = resources
    .filter((r) => r.category !== "Total" && r.contained_metal != null)
    .map((r) => ({
      label: `${r.category}`,
      contained: r.contained_metal,
      unit: r.contained_unit,
      fill: CATEGORY_COLORS[r.category] || "#6b7280",
    }));

  const drillChartData = drill_results
    .filter((d) => !d.is_including && d.au_gt != null && d.interval_m != null)
    .slice(0, 20)
    .map((d) => ({
      label: `${d.hole_id} ${d.from_m}m`,
      gm: (d.au_gt || 0) * (d.interval_m || 0),
      grade: d.au_gt,
    }));

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          href="/companies"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground mb-3"
        >
          <ArrowLeft className="h-3.5 w-3.5" /> Back to companies
        </Link>
        <div className="flex items-center gap-3">
          <h1 className="text-3xl font-bold font-mono text-primary">
            {ticker.toUpperCase()}
          </h1>
          {(company as Record<string, unknown>).name ? (
            <span className="text-lg text-muted-foreground">
              {String((company as Record<string, unknown>).name)}
            </span>
          ) : null}
          {projects[0] ? (
            <Badge variant="outline" className="text-xs capitalize">
              {String((projects[0] as Record<string, unknown>).stage)}
            </Badge>
          ) : null}
        </div>
      </div>

      {/* Valuation banner */}
      {valuation && (
        <Card className="bg-primary/5 border-primary/20">
          <CardContent className="py-4">
            <div className="flex items-center gap-2 mb-3">
              <TrendingUp className="h-4 w-4 text-primary" />
              <span className="text-sm font-semibold text-primary">
                Valuation — {valuation.method.replace(/_/g, " ")}
              </span>
            </div>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-5">
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  NAV
                </p>
                <p className="text-xl font-bold font-mono">
                  {fmtAud(valuation.nav_aud)}
                </p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  NAV/Share
                </p>
                <p className="text-xl font-bold font-mono">
                  {valuation.nav_per_share != null
                    ? `A$${valuation.nav_per_share.toFixed(4)}`
                    : "-"}
                </p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  Cash
                </p>
                <p className="text-lg font-mono">{fmtAud(valuation.cash_aud)}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  Attrib. Resource
                </p>
                <p className="text-lg font-mono">
                  {valuation.total_attributable_resource != null
                    ? `${valuation.total_attributable_resource.toFixed(1)} ${valuation.resource_unit || ""}`
                    : "-"}
                </p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  Shares FD
                </p>
                <p className="text-lg font-mono">
                  {valuation.shares_fd != null
                    ? `${(valuation.shares_fd / 1e6).toFixed(1)}M`
                    : "-"}
                </p>
              </div>
            </div>
            {valuation.red_flags.length > 0 && (
              <div className="mt-3 flex flex-wrap gap-2">
                {valuation.red_flags.map((flag, i) => (
                  <div
                    key={i}
                    className="inline-flex items-center gap-1 text-xs text-destructive bg-destructive/10 px-2 py-1 rounded-md"
                  >
                    <AlertTriangle className="h-3 w-3" />
                    {flag}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Tabs */}
      <Tabs defaultValue="resources" className="space-y-4">
        <TabsList>
          <TabsTrigger value="resources" className="gap-1.5">
            <Layers className="h-3.5 w-3.5" /> Resources
          </TabsTrigger>
          <TabsTrigger value="drill" className="gap-1.5">
            <Crosshair className="h-3.5 w-3.5" /> Drill Results
          </TabsTrigger>
          <TabsTrigger value="studies" className="gap-1.5">
            <FlaskConical className="h-3.5 w-3.5" /> Studies
          </TabsTrigger>
          <TabsTrigger value="financials" className="gap-1.5">
            <Landmark className="h-3.5 w-3.5" /> Financials
          </TabsTrigger>
          <TabsTrigger value="documents" className="gap-1.5">
            <FileText className="h-3.5 w-3.5" /> Documents
          </TabsTrigger>
        </TabsList>

        {/* Resources tab */}
        <TabsContent value="resources" className="space-y-4">
          {resourceChartData.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">
                  Resource by JORC Category
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-[250px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={resourceChartData} margin={{ top: 8, right: 8, bottom: 0, left: -8 }}>
                      <XAxis dataKey="label" tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 12 }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 12 }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: "8px", fontSize: "13px" }} />
                      <Bar dataKey="contained" radius={[4, 4, 0, 0]}>
                        {resourceChartData.map((entry, i) => (
                          <Cell key={i} fill={entry.fill} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          )}

          <Card>
            <CardContent className="p-0">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="px-4 py-3 font-medium">Category</th>
                    <th className="px-4 py-3 font-medium">Commodity</th>
                    <th className="px-4 py-3 font-medium text-right">Tonnes (Mt)</th>
                    <th className="px-4 py-3 font-medium text-right">Grade</th>
                    <th className="px-4 py-3 font-medium text-right">Contained</th>
                    <th className="px-4 py-3 font-medium text-right">Attributable</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {resources.map((r) => (
                    <tr key={r.id} className="hover:bg-muted/30">
                      <td className="px-4 py-2.5 font-medium">{r.category}</td>
                      <td className="px-4 py-2.5 capitalize text-muted-foreground">{r.commodity}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{r.tonnes_mt?.toFixed(1) ?? "-"}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{r.grade != null ? `${r.grade} ${r.grade_unit || ""}` : "-"}</td>
                      <td className="px-4 py-2.5 text-right font-mono">{r.contained_metal != null ? `${r.contained_metal.toFixed(0)} ${r.contained_unit || ""}` : "-"}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-primary">{r.attributable_contained != null ? `${r.attributable_contained.toFixed(0)} ${r.contained_unit || ""}` : "-"}</td>
                    </tr>
                  ))}
                  {resources.length === 0 && (
                    <tr><td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">No resource data</td></tr>
                  )}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Drill results tab */}
        <TabsContent value="drill" className="space-y-4">
          {drillChartData.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">
                  Top Intercepts by Gram-Metres
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-[400px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={drillChartData} layout="vertical" margin={{ top: 8, right: 16, bottom: 0, left: 120 }}>
                      <XAxis type="number" tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 11 }} axisLine={false} tickLine={false} />
                      <YAxis dataKey="label" type="category" tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 10 }} axisLine={false} tickLine={false} width={120} />
                      <Tooltip contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: "8px", fontSize: "13px" }} />
                      <Bar dataKey="gm" fill="hsl(var(--primary))" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          )}

          <Card>
            <CardContent className="p-0">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="px-4 py-3 font-medium">Hole</th>
                    <th className="px-4 py-3 font-medium text-right">From (m)</th>
                    <th className="px-4 py-3 font-medium text-right">To (m)</th>
                    <th className="px-4 py-3 font-medium text-right">Interval</th>
                    <th className="px-4 py-3 font-medium text-right">Au g/t</th>
                    <th className="px-4 py-3 font-medium text-right">AuEq g/t</th>
                    <th className="px-4 py-3 font-medium text-right">Sb %</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {drill_results.slice(0, 50).map((d, i) => (
                    <tr key={i} className={`hover:bg-muted/30 ${d.is_including ? "text-muted-foreground text-xs" : ""}`}>
                      <td className="px-4 py-2 font-mono text-xs">{d.is_including ? "  incl." : d.hole_id}</td>
                      <td className="px-4 py-2 text-right font-mono">{d.from_m?.toFixed(1) ?? "-"}</td>
                      <td className="px-4 py-2 text-right font-mono">{d.to_m?.toFixed(1) ?? "-"}</td>
                      <td className="px-4 py-2 text-right font-mono">{d.interval_m?.toFixed(1) ?? "-"}</td>
                      <td className="px-4 py-2 text-right font-mono font-semibold">{d.au_gt?.toFixed(2) ?? "-"}</td>
                      <td className="px-4 py-2 text-right font-mono text-primary">{d.au_eq_gt?.toFixed(2) ?? "-"}</td>
                      <td className="px-4 py-2 text-right font-mono">{d.sb_pct?.toFixed(2) ?? "-"}</td>
                    </tr>
                  ))}
                  {drill_results.length === 0 && (
                    <tr><td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">No drill results</td></tr>
                  )}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Studies tab */}
        <TabsContent value="studies">
          <div className="grid gap-4">
            {studies.map((s) => (
              <Card key={s.id}>
                <CardHeader className="pb-2">
                  <div className="flex items-center gap-2">
                    <CardTitle className="text-sm font-medium capitalize">
                      {s.study_stage || "Study"} — {s.project_name}
                    </CardTitle>
                    <Badge variant="outline" className="text-xs">
                      {s.study_date || "undated"}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-6">
                    {[
                      ["NPV", s.post_tax_npv_musd != null ? `US$${s.post_tax_npv_musd}M` : null],
                      ["IRR", s.irr_pct != null ? `${s.irr_pct}%` : null],
                      ["Capex", s.initial_capex_musd != null ? `US$${s.initial_capex_musd}M` : null],
                      ["Mine Life", s.mine_life_years != null ? `${s.mine_life_years} yrs` : null],
                      ["Recovery", s.recovery_pct != null ? `${s.recovery_pct}%` : null],
                      ["Assumed Price", s.assumed_commodity_price != null ? `${s.assumed_commodity_price} ${s.assumed_price_unit || ""}` : null],
                    ].map(([label, value]) => (
                      <div key={label as string}>
                        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
                        <p className="text-sm font-mono font-semibold">{(value as string) || "-"}</p>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            ))}
            {studies.length === 0 && (
              <Card>
                <CardContent className="py-8 text-center text-muted-foreground">No studies</CardContent>
              </Card>
            )}
          </div>
        </TabsContent>

        {/* Financials tab */}
        <TabsContent value="financials">
          <Card>
            <CardContent className="p-0">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="px-4 py-3 font-medium">Date</th>
                    <th className="px-4 py-3 font-medium text-right">Cash</th>
                    <th className="px-4 py-3 font-medium text-right">Debt</th>
                    <th className="px-4 py-3 font-medium text-right">Burn/Qtr</th>
                    <th className="px-4 py-3 font-medium text-right">Runway</th>
                    <th className="px-4 py-3 font-medium text-right">Shares FD</th>
                    <th className="px-4 py-3 font-medium text-right">Last Raise</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {financials.map((f, i) => {
                    const fin = f as Record<string, unknown>;
                    return (
                      <tr key={i} className="hover:bg-muted/30">
                        <td className="px-4 py-2.5 font-mono text-xs">{fin.effective_date as string || "-"}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(fin.cash_aud as number)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(fin.debt_aud as number)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(fin.quarterly_burn as number)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">
                          {fin.cash_runway_months != null ? (
                            <span className={(fin.cash_runway_months as number) < 6 ? "text-destructive" : (fin.cash_runway_months as number) < 12 ? "text-yellow-500" : "text-green-500"}>
                              {(fin.cash_runway_months as number).toFixed(1)}mo
                            </span>
                          ) : "-"}
                        </td>
                        <td className="px-4 py-2.5 text-right font-mono">{fin.shares_fd != null ? `${((fin.shares_fd as number) / 1e6).toFixed(1)}M` : "-"}</td>
                        <td className="px-4 py-2.5 text-right font-mono text-xs">{fin.last_raise_price != null ? `$${fin.last_raise_price}` : "-"}</td>
                      </tr>
                    );
                  })}
                  {financials.length === 0 && (
                    <tr><td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">No financial data</td></tr>
                  )}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Documents tab */}
        <TabsContent value="documents">
          <Card>
            <CardContent className="p-0">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="px-4 py-3 font-medium">Type</th>
                    <th className="px-4 py-3 font-medium">Header</th>
                    <th className="px-4 py-3 font-medium">Status</th>
                    <th className="px-4 py-3 font-medium">Date</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {documents.map((d) => (
                    <tr key={d.id} className="hover:bg-muted/30">
                      <td className="px-4 py-2.5"><Badge variant="secondary" className="text-xs">{d.doc_type}</Badge></td>
                      <td className="px-4 py-2.5 text-muted-foreground max-w-sm truncate">{d.header || "-"}</td>
                      <td className="px-4 py-2.5">
                        <span className={`text-xs font-medium ${d.parse_status === "done" ? "text-green-400" : d.parse_status === "failed" ? "text-red-400" : "text-yellow-400"}`}>
                          {d.parse_status}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground">{d.announcement_date || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
