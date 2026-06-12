"use client";

import Link from "next/link";

export default function ValuationsPage() {
  return (
    <div className="space-y-6 animate-fade-up">
      <div>
        <h1 className="q-display text-[28px] leading-none text-zinc-100">
          Valuations
        </h1>
        <p className="text-[13px] text-zinc-500 mt-1.5">
          Stage-based valuation engine
        </p>
      </div>

      <div className="q-card q-card-hero px-8 py-14 text-center">
        <p className="text-[13px] text-zinc-500">
          The standalone valuation screen is not yet available.
        </p>
        <p className="text-[12px] text-zinc-600 mt-1.5">
          Spot revaluations are live on the{" "}
          <Link
            href="/companies"
            className="text-amber/80 hover:text-amber transition-colors"
          >
            Companies
          </Link>{" "}
          screener and on each company&apos;s Operations tab.
        </p>
      </div>
    </div>
  );
}
