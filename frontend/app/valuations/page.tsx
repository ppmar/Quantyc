"use client";

import { Card, CardContent } from "@/components/ui/card";
import { TrendingUp } from "lucide-react";

export default function ValuationsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Valuations</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Stage-based fair value estimates
        </p>
      </div>

      <Card>
        <CardContent className="py-16 text-center">
          <TrendingUp className="h-12 w-12 mx-auto text-muted-foreground/30 mb-4" />
          <p className="text-lg font-medium text-muted-foreground">
            Valuation engine coming soon
          </p>
          <p className="text-sm text-muted-foreground/70 mt-1">
            Requires resources, studies, and project data to produce NAV and EV/resource estimates.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
