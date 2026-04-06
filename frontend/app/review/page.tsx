"use client";

import { useEffect, useState } from "react";
import { api, type ReviewData } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { AlertTriangle, XCircle, FileWarning, Database, FlaskConical, Layers } from "lucide-react";
import Link from "next/link";

export default function ReviewPage() {
  const [data, setData] = useState<ReviewData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .review()
      .then(setData)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  if (!data) return null;

  const totalIssues =
    data.staging.length +
    data.financials.length +
    data.resources.length +
    data.studies.length +
    data.failed_docs.length +
    data.red_flags.length;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Review</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {totalIssues} items need attention
        </p>
      </div>

      {/* Red flags prominent */}
      {data.red_flags.length > 0 && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-destructive flex items-center gap-1.5">
              <AlertTriangle className="h-4 w-4" />
              Red Flags ({data.red_flags.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {data.red_flags.map((flag, i) => (
                <div key={i} className="flex items-start gap-3 text-sm">
                  <Link href={`/company/${flag.ticker}`}>
                    <Badge variant="secondary" className="font-mono text-xs shrink-0 hover:bg-primary/20">
                      {flag.ticker}
                    </Badge>
                  </Link>
                  <div>
                    <span className="text-muted-foreground">{flag.description}</span>
                    <Badge variant="outline" className="ml-2 text-[10px]">
                      {flag.flag_type}
                    </Badge>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <Tabs defaultValue="failed" className="space-y-4">
        <TabsList>
          <TabsTrigger value="failed" className="gap-1.5">
            <XCircle className="h-3.5 w-3.5" /> Failed ({data.failed_docs.length})
          </TabsTrigger>
          <TabsTrigger value="staging" className="gap-1.5">
            <Database className="h-3.5 w-3.5" /> Staging ({data.staging.length})
          </TabsTrigger>
          <TabsTrigger value="financials" className="gap-1.5">
            <FileWarning className="h-3.5 w-3.5" /> Financials ({data.financials.length})
          </TabsTrigger>
          <TabsTrigger value="resources" className="gap-1.5">
            <Layers className="h-3.5 w-3.5" /> Resources ({data.resources.length})
          </TabsTrigger>
          <TabsTrigger value="studies" className="gap-1.5">
            <FlaskConical className="h-3.5 w-3.5" /> Studies ({data.studies.length})
          </TabsTrigger>
        </TabsList>

        <TabsContent value="failed">
          <Card>
            <CardContent className="p-0">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="px-4 py-3 font-medium">Ticker</th>
                    <th className="px-4 py-3 font-medium">Type</th>
                    <th className="px-4 py-3 font-medium">Header</th>
                    <th className="px-4 py-3 font-medium">Date</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {data.failed_docs.map((d, i) => (
                    <tr key={i} className="hover:bg-muted/30">
                      <td className="px-4 py-2.5">
                        <Link href={`/company/${d.company_ticker}`} className="font-mono text-primary hover:underline">
                          {d.company_ticker as string}
                        </Link>
                      </td>
                      <td className="px-4 py-2.5"><Badge variant="secondary" className="text-xs">{d.doc_type as string}</Badge></td>
                      <td className="px-4 py-2.5 text-muted-foreground">{d.header as string || "-"}</td>
                      <td className="px-4 py-2.5 font-mono text-xs">{d.announcement_date as string || "-"}</td>
                    </tr>
                  ))}
                  {data.failed_docs.length === 0 && (
                    <tr><td colSpan={4} className="px-4 py-8 text-center text-muted-foreground">No failed documents</td></tr>
                  )}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="staging">
          <Card>
            <CardContent className="p-0">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="px-4 py-3 font-medium">Ticker</th>
                    <th className="px-4 py-3 font-medium">Field</th>
                    <th className="px-4 py-3 font-medium">Value</th>
                    <th className="px-4 py-3 font-medium">Method</th>
                    <th className="px-4 py-3 font-medium">Confidence</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {data.staging.map((s, i) => (
                    <tr key={i} className="hover:bg-muted/30">
                      <td className="px-4 py-2.5 font-mono text-primary">{s.company_ticker as string}</td>
                      <td className="px-4 py-2.5 font-mono text-xs">{s.field_name as string}</td>
                      <td className="px-4 py-2.5">{s.raw_value as string} <span className="text-muted-foreground text-xs">{s.unit as string}</span></td>
                      <td className="px-4 py-2.5"><Badge variant="outline" className="text-xs">{s.extraction_method as string}</Badge></td>
                      <td className="px-4 py-2.5"><Badge variant={s.confidence === "low" ? "destructive" : "secondary"} className="text-xs">{s.confidence as string}</Badge></td>
                    </tr>
                  ))}
                  {data.staging.length === 0 && (
                    <tr><td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">No staging items need review</td></tr>
                  )}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="financials">
          <Card>
            <CardContent className={data.financials.length === 0 ? "py-8 text-center text-muted-foreground" : "p-0"}>
              {data.financials.length === 0 ? "No financial records need review" : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-muted-foreground">
                      <th className="px-4 py-3 font-medium">Ticker</th>
                      <th className="px-4 py-3 font-medium">Date</th>
                      <th className="px-4 py-3 font-medium text-right">Cash</th>
                      <th className="px-4 py-3 font-medium text-right">Runway</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border/50">
                    {data.financials.map((f, i) => (
                      <tr key={i} className="hover:bg-muted/30">
                        <td className="px-4 py-2.5 font-mono text-primary">{f.ticker as string}</td>
                        <td className="px-4 py-2.5 font-mono text-xs">{f.effective_date as string}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{f.cash_aud != null ? `A$${((f.cash_aud as number) / 1e6).toFixed(1)}M` : "-"}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{f.cash_runway_months != null ? `${(f.cash_runway_months as number).toFixed(1)}mo` : "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="resources">
          <Card>
            <CardContent className={data.resources.length === 0 ? "py-8 text-center text-muted-foreground" : "p-0"}>
              {data.resources.length === 0 ? "No resource records need review" : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-muted-foreground">
                      <th className="px-4 py-3 font-medium">Ticker</th>
                      <th className="px-4 py-3 font-medium">Commodity</th>
                      <th className="px-4 py-3 font-medium">Category</th>
                      <th className="px-4 py-3 font-medium text-right">Contained</th>
                      <th className="px-4 py-3 font-medium">Confidence</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border/50">
                    {data.resources.map((r, i) => (
                      <tr key={i} className="hover:bg-muted/30">
                        <td className="px-4 py-2.5 font-mono text-primary">{r.ticker as string}</td>
                        <td className="px-4 py-2.5 capitalize">{r.commodity as string}</td>
                        <td className="px-4 py-2.5">{r.category as string}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{r.contained_metal as number} {r.contained_unit as string}</td>
                        <td className="px-4 py-2.5"><Badge variant="outline" className="text-xs">{r.confidence as string}</Badge></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="studies">
          <Card>
            <CardContent className={data.studies.length === 0 ? "py-8 text-center text-muted-foreground" : "p-0"}>
              {data.studies.length === 0 ? "No study records need review" : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-muted-foreground">
                      <th className="px-4 py-3 font-medium">Ticker</th>
                      <th className="px-4 py-3 font-medium">Stage</th>
                      <th className="px-4 py-3 font-medium">Date</th>
                      <th className="px-4 py-3 font-medium text-right">NPV</th>
                      <th className="px-4 py-3 font-medium text-right">IRR</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border/50">
                    {data.studies.map((s, i) => (
                      <tr key={i} className="hover:bg-muted/30">
                        <td className="px-4 py-2.5 font-mono text-primary">{s.ticker as string}</td>
                        <td className="px-4 py-2.5 capitalize">{s.study_stage as string}</td>
                        <td className="px-4 py-2.5 font-mono text-xs">{s.study_date as string}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{s.post_tax_npv_musd != null ? `US$${s.post_tax_npv_musd}M` : "-"}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{s.irr_pct != null ? `${s.irr_pct}%` : "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
