"use client";

import { useEffect, useState, use } from "react";
import {
  api,
  type FinancialsResponse,
  type Document,
} from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  ArrowLeft,
  FileText,
  Landmark,
} from "lucide-react";
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
  if (val >= 1e3) return `${(val / 1e3).toFixed(0)}K`;
  return val.toLocaleString();
}

export default function CompanyPage({
  params,
}: {
  params: Promise<{ ticker: string }>;
}) {
  const { ticker } = use(params);
  const [financials, setFinancials] = useState<FinancialsResponse | null>(null);
  const [documents, setDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.financials(ticker).catch(() => null),
      api.documents({ ticker }).catch(() => []),
    ])
      .then(([fin, docs]) => {
        setFinancials(fin);
        setDocuments(docs);
      })
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  const latest = financials?.latest ?? null;
  const history = financials?.history ?? [];
  const companyName = latest?.name ?? null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          href="/"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground mb-3"
        >
          <ArrowLeft className="h-3.5 w-3.5" /> Back to dashboard
        </Link>
        <div className="flex items-center gap-3">
          <h1 className="text-3xl font-bold font-mono text-primary">
            {ticker.toUpperCase()}
          </h1>
          {companyName && (
            <span className="text-lg text-muted-foreground">{companyName}</span>
          )}
        </div>
      </div>

      {/* Latest snapshot */}
      {latest && (
        <Card className="bg-primary/5 border-primary/20">
          <CardContent className="py-4">
            <p className="text-xs uppercase tracking-wider text-muted-foreground mb-3">
              Latest snapshot — {latest.effective_date}
            </p>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 lg:grid-cols-6">
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Cash</p>
                <p className="text-xl font-bold font-mono">{fmtAud(latest.cash)}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Debt</p>
                <p className="text-lg font-mono">{fmtAud(latest.debt)}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Opex Burn/Qtr</p>
                <p className="text-lg font-mono">{fmtAud(latest.quarterly_opex_burn)}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Shares Basic</p>
                <p className="text-lg font-mono">{fmtShares(latest.shares_basic)}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Shares FD</p>
                <p className="text-lg font-mono">{fmtShares(latest.shares_fd)}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Confidence</p>
                <Badge variant="outline" className="text-xs mt-1">
                  {latest.confidence}
                </Badge>
              </div>
            </div>
            {latest.needs_review === 1 && latest.review_reason && (
              <p className="mt-3 text-xs text-destructive bg-destructive/10 px-2 py-1 rounded-md inline-block">
                {latest.review_reason}
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Tabs */}
      <Tabs defaultValue="financials" className="space-y-4">
        <TabsList>
          <TabsTrigger value="financials" className="gap-1.5">
            <Landmark className="h-3.5 w-3.5" /> Financials
          </TabsTrigger>
          <TabsTrigger value="documents" className="gap-1.5">
            <FileText className="h-3.5 w-3.5" /> Documents ({documents.length})
          </TabsTrigger>
        </TabsList>

        {/* Financials tab */}
        <TabsContent value="financials">
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-muted-foreground">
                      <th className="px-4 py-3 font-medium">Date</th>
                      <th className="px-4 py-3 font-medium text-right">Cash</th>
                      <th className="px-4 py-3 font-medium text-right">Debt</th>
                      <th className="px-4 py-3 font-medium text-right">Opex/Qtr</th>
                      <th className="px-4 py-3 font-medium text-right">Invest/Qtr</th>
                      <th className="px-4 py-3 font-medium text-right">Shares FD</th>
                      <th className="px-4 py-3 font-medium text-right">Options</th>
                      <th className="px-4 py-3 font-medium">Method</th>
                      <th className="px-4 py-3 font-medium">Review</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border/50">
                    {history.map((f) => (
                      <tr key={f.financial_id} className="hover:bg-muted/30">
                        <td className="px-4 py-2.5 font-mono text-xs">{f.effective_date || "-"}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(f.cash)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(f.debt)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(f.quarterly_opex_burn)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtAud(f.quarterly_invest_burn)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtShares(f.shares_fd)}</td>
                        <td className="px-4 py-2.5 text-right font-mono">{fmtShares(f.options_outstanding)}</td>
                        <td className="px-4 py-2.5">
                          <Badge variant="secondary" className="text-[10px]">{f.extraction_method}</Badge>
                        </td>
                        <td className="px-4 py-2.5">
                          {f.needs_review === 1 ? (
                            <span className="text-xs text-destructive" title={f.review_reason || ""}>flagged</span>
                          ) : f.reviewed_at ? (
                            <span className="text-xs text-green-400">reviewed</span>
                          ) : (
                            <span className="text-xs text-muted-foreground">ok</span>
                          )}
                        </td>
                      </tr>
                    ))}
                    {history.length === 0 && (
                      <tr>
                        <td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">
                          No financial data
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
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
                    <th className="px-4 py-3 font-medium">Link</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  {documents.map((d) => (
                    <tr key={d.document_id} className="hover:bg-muted/30">
                      <td className="px-4 py-2.5">
                        <Badge variant="secondary" className="text-xs">{d.doc_type}</Badge>
                      </td>
                      <td className="px-4 py-2.5 text-muted-foreground max-w-sm truncate">
                        {d.header || "-"}
                      </td>
                      <td className="px-4 py-2.5">
                        <span
                          className={`text-xs font-medium ${
                            d.parse_status === "parsed"
                              ? "text-green-400"
                              : d.parse_status === "failed"
                              ? "text-red-400"
                              : d.parse_status === "classified"
                              ? "text-blue-400"
                              : "text-yellow-400"
                          }`}
                        >
                          {d.parse_status}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground">
                        {d.announcement_date || "-"}
                      </td>
                      <td className="px-4 py-2.5">
                        {d.url && !d.url.startsWith("upload://") ? (
                          <a
                            href={d.url}
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
                  ))}
                  {documents.length === 0 && (
                    <tr>
                      <td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                        No documents
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
