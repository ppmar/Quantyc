"use client";

import { useEffect, useState } from "react";
import { api, type Company } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import Link from "next/link";

const STAGE_COLORS: Record<string, string> = {
  concept: "bg-gray-500/10 text-gray-400 border-gray-500/20",
  discovery: "bg-blue-500/10 text-blue-400 border-blue-500/20",
  feasibility: "bg-amber-500/10 text-amber-400 border-amber-500/20",
  development: "bg-purple-500/10 text-purple-400 border-purple-500/20",
  production: "bg-green-500/10 text-green-400 border-green-500/20",
};

export default function CompaniesPage() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .companies()
      .then(setCompanies)
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
        <h1 className="text-2xl font-bold tracking-tight">Companies</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {companies.length} companies tracked
        </p>
      </div>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {companies.map((c) => (
          <Link key={c.ticker} href={`/company/${c.ticker}`}>
            <Card className="hover:border-primary/30 transition-all hover:shadow-lg hover:shadow-primary/5 cursor-pointer h-full">
              <CardContent className="p-5">
                <div className="flex items-start justify-between">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="text-lg font-bold font-mono text-primary">
                        {c.ticker}
                      </span>
                      {c.stage && (
                        <span
                          className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${
                            STAGE_COLORS[c.stage] || STAGE_COLORS.concept
                          }`}
                        >
                          {c.stage}
                        </span>
                      )}
                    </div>
                    {c.name && (
                      <p className="text-sm text-muted-foreground mt-0.5">
                        {c.name}
                      </p>
                    )}
                  </div>
                  {c.primary_commodity && (
                    <Badge variant="outline" className="text-xs capitalize">
                      {c.primary_commodity}
                    </Badge>
                  )}
                </div>

                <div className="mt-4 grid grid-cols-3 gap-3">
                  <div>
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                      Docs
                    </p>
                    <p className="text-sm font-semibold font-mono">
                      {c.parsed_count}/{c.doc_count}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                      Cash
                    </p>
                    <p className="text-sm font-semibold font-mono">
                      {c.cash ? `$${(c.cash / 1e6).toFixed(1)}M` : "-"}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                      Runway
                    </p>
                    <p
                      className={`text-sm font-semibold font-mono ${
                        c.runway != null
                          ? c.runway < 6
                            ? "text-destructive"
                            : c.runway < 12
                            ? "text-yellow-500"
                            : "text-green-500"
                          : "text-muted-foreground"
                      }`}
                    >
                      {c.runway != null ? `${c.runway.toFixed(1)}mo` : "-"}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
