"use client";

import { useState, use } from "react";
import { useCompanySnapshot } from "@/hooks/useCompanySnapshot";
import { CompanyHeader } from "@/components/company/CompanyHeader";
import { TabBar } from "@/components/company/TabBar";
import { HeroGrid } from "@/components/company/HeroGrid";
import { CashTrajectoryChart } from "@/components/company/CashTrajectoryChart";
import { ActivityFeed } from "@/components/company/ActivityFeed";
import { SnapshotSkeleton } from "@/components/company/Skeleton";
import { OperationsTab } from "./OperationsTab";

export default function CompanyPage({
  params,
}: {
  params: Promise<{ ticker: string }>;
}) {
  const { ticker } = use(params);
  const snapshot = useCompanySnapshot(ticker);
  const [activeTab, setActiveTab] = useState("summary");

  // Loading state — skeleton matching final layout shape
  if (snapshot.status === "loading") {
    return (
      <div className="tabular-nums max-w-4xl">
        <SnapshotSkeleton />
      </div>
    );
  }

  // Error state — section-level error with retry
  if (snapshot.status === "error") {
    return (
      <div className="max-w-4xl">
        <CompanyHeader
          ticker={ticker.toUpperCase()}
          name={ticker.toUpperCase()}
          metaLine=""
        />
        <div className="mt-12 text-center py-16">
          <p className="text-zinc-400 text-sm mb-4">
            Failed to load data for this ticker.
          </p>
          <button
            onClick={snapshot.retry}
            className="text-sm text-zinc-300 hover:text-zinc-100 underline underline-offset-4 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  const data = snapshot.data;

  // Empty state — no data yet
  if (!data.has_data) {
    return (
      <div className="tabular-nums max-w-4xl">
        <CompanyHeader
          ticker={data.ticker}
          name={data.name}
          metaLine={data.meta_line}
        />
        <p className="mt-12 text-sm text-zinc-500">
          Documents are being processed for this ticker.
        </p>
      </div>
    );
  }

  return (
    <div className="tabular-nums max-w-4xl space-y-10">
      <CompanyHeader
        ticker={data.ticker}
        name={data.name}
        metaLine={data.meta_line}
      />

      <TabBar tabs={data.tabs} active={activeTab} onChange={setActiveTab} />

      {activeTab === "summary" && (
        <div className="space-y-10">
          <HeroGrid cash={data.cash} capital={data.capital} />

          <div className="border-t border-white/[0.06]" />

          <CashTrajectoryChart data={data.cash_history} />

          {data.cash_history.length >= 3 && data.activity.length > 0 && (
            <div className="border-t border-white/[0.06]" />
          )}

          <ActivityFeed events={data.activity} ticker={data.ticker} />
        </div>
      )}

      {activeTab === "operations" && (
        <OperationsTab projects={data.projects ?? []} />
      )}
    </div>
  );
}
