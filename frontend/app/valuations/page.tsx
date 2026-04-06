"use client";

import { useEffect, useState } from "react";
import { api, type Valuation } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import Link from "next/link";
import { AlertTriangle, TrendingUp } from "lucide-react";

function fmtAud(val: number | null) {
  if (val == null) return "-";
  if (Math.abs(val) >= 1e9) return `A$${(val / 1e9).toFixed(2)}B`;
  if (Math.abs(val) >= 1e6) return `A$${(val / 1e6).toFixed(2)}M`;
  if (Math.abs(val) >= 1e3) return `A$${(val / 1e3).toFixed(1)}K`;
  return `A$${val.toFixed(2)}`;
}

export default function ValuationsPage() {
  const [valuations, setValuations] = useState<Valuation[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .valuations()
      .then(setValuations)
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
        <h1 className="text-2xl font-bold tracking-tight">Valuations</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Stage-based fair value estimates
        </p>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {valuations.map((v) => (
          <Card
            key={v.ticker}
            className={
              v.red_flags.length > 0 ? "border-destructive/20" : ""
            }
          >
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <Link
                    href={`/company/${v.ticker}`}
                    className="text-lg font-bold font-mono text-primary hover:underline"
                  >
                    {v.ticker}
                  </Link>
                  <Badge variant="outline" className="text-xs capitalize">
                    {v.stage}
                  </Badge>
                </div>
                <Badge variant="secondary" className="text-xs">
                  {v.method.replace(/_/g, " ")}
                </Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    NAV
                  </p>
                  <p className="text-base font-bold font-mono">
                    {fmtAud(v.nav_aud)}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    NAV/Share
                  </p>
                  <p className="text-base font-bold font-mono">
                    {v.nav_per_share != null
                      ? `A$${v.nav_per_share.toFixed(4)}`
                      : "-"}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    Cash
                  </p>
                  <p className="text-sm font-mono text-muted-foreground">
                    {fmtAud(v.cash_aud)}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    Shares FD
                  </p>
                  <p className="text-sm font-mono text-muted-foreground">
                    {v.shares_fd != null
                      ? `${(v.shares_fd / 1e6).toFixed(1)}M`
                      : "-"}
                  </p>
                </div>
              </div>

              {v.total_attributable_resource != null && (
                <div className="mt-3 flex items-center gap-2 text-sm">
                  <TrendingUp className="h-3.5 w-3.5 text-primary" />
                  <span className="text-muted-foreground">
                    Attributable resource:{" "}
                    <span className="font-mono font-semibold text-foreground">
                      {v.total_attributable_resource.toFixed(1)}{" "}
                      {v.resource_unit || "units"}
                    </span>
                  </span>
                </div>
              )}

              {v.red_flags.length > 0 && (
                <div className="mt-3 space-y-1">
                  {v.red_flags.map((flag, i) => (
                    <div
                      key={i}
                      className="flex items-start gap-1.5 text-xs text-destructive"
                    >
                      <AlertTriangle className="h-3 w-3 mt-0.5 shrink-0" />
                      {flag}
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      {valuations.length === 0 && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            No valuations yet. Run the pipeline and load data first.
          </CardContent>
        </Card>
      )}
    </div>
  );
}
