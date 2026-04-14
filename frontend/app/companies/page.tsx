"use client";

import { useEffect, useState } from "react";
import { api, type Company } from "@/lib/api";
import { Card, CardContent } from "@/components/ui/card";
import Link from "next/link";

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
                    <span className="text-lg font-bold font-mono text-primary">
                      {c.ticker}
                    </span>
                    {c.name && (
                      <p className="text-sm text-muted-foreground mt-0.5">
                        {c.name}
                      </p>
                    )}
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {c.reporting_currency}
                  </span>
                </div>

                <div className="mt-4 grid grid-cols-2 gap-3">
                  <div>
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                      Documents
                    </p>
                    <p className="text-sm font-semibold font-mono">
                      {c.doc_count}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
                      Parsed
                    </p>
                    <p className="text-sm font-semibold font-mono">
                      {c.parsed_count}/{c.doc_count}
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
