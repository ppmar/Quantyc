"use client";

import React from "react";
import type { ProjectData, StudyData, RevaluationData } from "@/types/snapshot";

// ─── Presentation helpers ──────────────────────────────────────

const METHOD_VERSION_LABELS: Record<string, string> = {
  first_order_v1: "Price sensitivity \u00b7 1st order",
  first_order_v2: "Price sensitivity \u00b7 1st order",
};

function fmtMethodVersion(raw: string | null | undefined): string {
  if (!raw) return "";
  return METHOD_VERSION_LABELS[raw] ?? "Modelled";
}

const SPOT_SOURCE_LABELS: Record<string, string> = {
  "yahoo:GC=F": "Gold spot (Yahoo)",
  "yahoo:HG=F": "Copper spot (Yahoo)",
  "yahoo:AUDUSD=X": "AUD/USD (Yahoo)",
};

function fmtSpotSource(raw: string | null | undefined): string {
  if (!raw) return "";
  return SPOT_SOURCE_LABELS[raw] ?? raw;
}

function fmtPriceForCommodity(price: number, commodity: string): string {
  const digits = commodity === "Cu" ? 2 : 0;
  return price.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtPerUnitUSD(val: number | null, unit: string | null): string {
  if (val == null) return "—";
  const denom = (unit ?? "").replace(/^USD\s*\/\s*/i, "/");
  const digits = Math.abs(val) >= 100 ? 0 : 2;
  return `US$${val.toFixed(digits)}${denom}`;
}

// Single-accent palette per U2.
function stageBadge(stage: string | null) {
  if (!stage) return null;
  const s = stage.toLowerCase();

  const STYLES: Record<string, string> = {
    production:            "text-amber-300 bg-amber-500/10 border-amber-700/40",
    development:           "text-amber-400/90 border-amber-800/60",
    feasibility:           "text-amber-400/90 border-amber-800/60",
    advanced_exploration:  "text-zinc-300 border-zinc-700",
    discovery:             "text-zinc-300 border-zinc-700",
    exploration:           "text-zinc-400 border-zinc-700",
    concept:               "text-zinc-500 border-zinc-800",
    care_and_maintenance:  "text-zinc-500 border-zinc-800",
  };
  const cls = STYLES[s] ?? "text-zinc-400 border-zinc-700";
  const label = stage.replace(/_/g, " ");

  return (
    <span className={`text-[10px] uppercase tracking-wider border px-1.5 py-0.5 rounded ${cls}`}>
      {label}
    </span>
  );
}

// ─── DFS vintage signal ────────────────────────────────────────

function computeStudyVintage(isoDate: string | null): { years: number; tier: "fresh" | "aging" | "stale" } | null {
  if (!isoDate) return null;
  const d = new Date(isoDate);
  if (isNaN(d.getTime())) return null;
  const years = (Date.now() - d.getTime()) / (365.25 * 24 * 3600 * 1000);
  if (years < 0) return null;   // future-dated study: no vintage badge (defensive; backend I3 should prevent this)
  let tier: "fresh" | "aging" | "stale";
  if (years < 3) tier = "fresh";
  else if (years < 5) tier = "aging";
  else tier = "stale";
  return { years, tier };
}

function StudyVintageBadge({ isoDate }: { isoDate: string | null }) {
  const v = computeStudyVintage(isoDate);
  if (!v) return null;
  const yrs = v.years.toFixed(0);

  if (v.tier === "fresh") {
    return <span className="text-[10px] text-zinc-600">&middot; {yrs}y old</span>;
  }
  if (v.tier === "aging") {
    return <span className="text-[10px] text-zinc-500">&middot; {yrs}y old</span>;
  }
  return (
    <span
      className="text-[10px] text-amber-400/80 border border-amber-900/60 rounded px-1.5 py-0.5"
      title="Study is ≥ 5 years old. Cost and capex assumptions are likely stale; a restudy may materially change the economics."
    >
      {yrs}y old &middot; restudy likely
    </span>
  );
}

// ─── Methodology hint ──────────────────────────────────────────

function MethodologyHint() {
  return (
    <span
      className="inline-flex items-center justify-center w-3.5 h-3.5 rounded-full border border-zinc-700 text-zinc-500 text-[9px] leading-none cursor-help hover:text-zinc-300 hover:border-zinc-500 transition-colors"
      title={
        "First-order price sensitivity:\n" +
        "  ΔNPV ≈ (P_spot − P_DFS) × annual production × annuity factor × (1 − tax)\n" +
        "Holds constant: AISC, capex, royalties, recovery, mine life, production schedule.\n" +
        "Not a full DCF re-model. Aged DFS publications and price ratios > 2× erode confidence."
      }
      aria-label="Methodology explanation"
    >
      i
    </span>
  );
}

function fmtMoney(val: number | null, currency: string | null) {
  if (val == null) return "—";
  const sym = currency === "USD" ? "US$" : currency === "AUD" ? "A$" : `${currency ?? ""}$`;
  if (Math.abs(val) >= 1000) return `${sym}${(val / 1000).toFixed(2)}B`;
  return `${sym}${val.toFixed(0)}M`;
}

function fmtPct(val: number | null) {
  if (val == null) return "—";
  return `${val.toFixed(1)}%`;
}

function StudyCard({ study, suppressCommodities }: { study: StudyData; suppressCommodities?: Set<string> }) {
  const ccy = study.reporting_currency;
  const dr = study.discount_rate_pct != null ? study.discount_rate_pct.toFixed(0) : "?";

  return (
    <div className="mt-4">
      <div className="flex items-center gap-2 mb-3 flex-wrap">
        <h4 className="text-xs font-medium text-zinc-400 uppercase tracking-wider">{study.study_type}</h4>
        {study.study_date && (
          <span className="text-[10px] text-zinc-600">{study.study_date}</span>
        )}
        <StudyVintageBadge isoDate={study.study_date_iso ?? null} />
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 overflow-hidden rounded-md border border-white/[0.05] divide-x divide-y divide-white/[0.04] [&>div]:px-3.5 [&>div]:py-2.5 [&>div]:bg-white/[0.015]">
        {study.post_tax_npv != null && (
          <div>
            <p className="q-label">NPV{dr} post-tax</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{fmtMoney(study.post_tax_npv, ccy)}</p>
          </div>
        )}
        {study.pre_tax_npv != null && (
          <div>
            <p className="q-label">NPV{dr} pre-tax</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{fmtMoney(study.pre_tax_npv, ccy)}</p>
          </div>
        )}
        {study.irr_pct != null && (
          <div>
            <p className="q-label">IRR</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{fmtPct(study.irr_pct)}</p>
          </div>
        )}
        {study.initial_capex != null && (
          <div>
            <p className="q-label">Initial capex</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{fmtMoney(study.initial_capex, ccy)}</p>
          </div>
        )}
        {study.aisc_per_unit != null && (
          <div>
            <p className="q-label">AISC</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{fmtPerUnitUSD(study.aisc_per_unit, study.aisc_unit)}</p>
          </div>
        )}
        {study.mine_life_years != null && (
          <div>
            <p className="q-label">Mine life</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{study.mine_life_years.toFixed(0)} years</p>
          </div>
        )}
        {study.payback_years != null && (
          <div>
            <p className="q-label">Payback</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{study.payback_years.toFixed(1)} years</p>
          </div>
        )}
        {study.recovery_pct != null && (
          <div>
            <p className="q-label">Recovery</p>
            <p className="mt-1 font-mono text-sm text-zinc-100">{fmtPct(study.recovery_pct)}</p>
          </div>
        )}
      </div>

      {study.price_assumptions && study.price_assumptions.length > 0 && (
        <div className="mt-3 flex gap-3 flex-wrap">
          {study.price_assumptions
            .filter((pa) => !suppressCommodities?.has(pa.commodity))
            .map((pa, i) => (
              <span key={i} className="text-[10px] text-zinc-500 border border-white/[0.06] rounded px-1.5 py-0.5">
                {pa.commodity} @ {pa.price.toLocaleString()} {pa.unit}
              </span>
            ))}
        </div>
      )}
    </div>
  );
}

function signalColor(upliftPct: number) {
  if (upliftPct > 0.5) return "text-emerald-400";
  if (upliftPct > 0.15) return "text-emerald-500";
  if (upliftPct > 0) return "text-zinc-300";
  if (upliftPct > -0.15) return "text-zinc-400";
  if (upliftPct > -0.5) return "text-amber-400";
  return "text-red-400";
}

// Left signal rail color for the reval card.
function signalRail(upliftPct: number) {
  if (upliftPct > 0.15)
    return "bg-emerald-400 shadow-[0_0_10px_oklch(0.72_0.19_145/0.5)]";
  if (upliftPct > -0.15) return "bg-zinc-600";
  if (upliftPct > -0.5)
    return "bg-amber shadow-[0_0_10px_oklch(0.795_0.155_85/0.5)]";
  return "bg-red-400 shadow-[0_0_10px_oklch(0.63_0.21_25/0.5)]";
}

function NPVBar({
  label,
  value,
  max,
  ccy,
  emphasis,
  barClass,
}: {
  label: string;
  value: number;
  max: number;
  ccy: string;
  emphasis?: boolean;
  barClass: string;
}) {
  const widthPct = max > 0 ? Math.max((Math.abs(value) / max) * 100, 2) : 2;
  return (
    <div className="flex items-center gap-3">
      <span className="q-label w-24 shrink-0">{label}</span>
      <div className="relative h-5 flex-1 overflow-hidden rounded-sm bg-white/[0.03]">
        <div
          className={`absolute inset-y-0 left-0 rounded-sm transition-[width] duration-700 ease-out ${barClass}`}
          style={{ width: `${widthPct}%` }}
        />
      </div>
      <span
        className={`w-24 shrink-0 text-right font-mono text-[13px] tabular-nums ${
          emphasis ? "text-zinc-100" : "text-zinc-500"
        }`}
      >
        {value < 0 ? "-" : ""}
        {ccy}
        {Math.abs(value).toLocaleString(undefined, { maximumFractionDigits: 0 })}M
      </span>
    </div>
  );
}

function RevaluationCard({ reval }: { reval: RevaluationData }) {
  const ccy = reval.reporting_currency === "USD" ? "US$" : reval.reporting_currency === "AUD" ? "A$" : `${reval.reporting_currency ?? ""}$`;
  const upliftPct = reval.npv_uplift_pct;
  const priceChangePct = ((reval.price_spot - reval.price_dfs) / reval.price_dfs) * 100;
  // A huge % on a tiny NPV base is not a strong signal — de-emphasise it (I6).
  const lowBase = reval.npv_dfs != null && reval.npv_dfs < 50;
  const upliftColor = lowBase ? "text-zinc-400" : signalColor(upliftPct);
  // Label the assumed price by the real study tier, not hardcoded "DFS" (I6).
  const assumedLabel =
    reval.study_confidence_tier === "definitive" ? "DFS deck"
    : reval.study_confidence_tier === "indicative" ? "PFS deck"
    : "Study deck";

  const spotDate = reval.spot_fetched_at
    ? new Date(reval.spot_fetched_at).toLocaleDateString("en-AU", { day: "numeric", month: "short", year: "numeric" })
    : "";

  const maxNPV = Math.max(Math.abs(reval.npv_dfs), Math.abs(reval.npv_spot));

  return (
    <div className="relative mt-5 overflow-hidden rounded-lg border border-white/[0.06] bg-white/[0.015] p-4 pl-5">
      {/* signal rail */}
      <span
        className={`absolute left-0 top-0 bottom-0 w-[3px] ${signalRail(lowBase ? 0 : upliftPct)}`}
      />

      <div className="mb-4 flex items-start justify-between gap-4">
        <div className="flex items-center gap-1.5">
          <h4 className="text-xs font-medium text-zinc-400 uppercase tracking-wider">
            Spot revaluation
          </h4>
          <MethodologyHint />
          {lowBase && (
            <span className="q-label border border-white/[0.08] rounded px-1.5 py-0.5">
              small base
            </span>
          )}
        </div>
        <div className="text-right">
          <span className={`font-mono text-[26px] leading-none tabular-nums ${upliftColor}`}>
            {upliftPct >= 0 ? "+" : ""}
            {(upliftPct * 100).toFixed(0)}%
          </span>
          <p className={`mt-0.5 font-mono text-[12px] tabular-nums ${upliftColor}`}>
            {reval.npv_uplift >= 0 ? "+" : ""}
            {ccy}
            {reval.npv_uplift.toLocaleString(undefined, { maximumFractionDigits: 0 })}M
          </p>
        </div>
      </div>

      {/* NPV bars — study vs spot at one glance */}
      <div className="space-y-2">
        <NPVBar
          label={`NPV · ${assumedLabel.split(" ")[0]}`}
          value={reval.npv_dfs}
          max={maxNPV}
          ccy={ccy}
          barClass="bg-zinc-600/70"
        />
        <NPVBar
          label="NPV · spot"
          value={reval.npv_spot}
          max={maxNPV}
          ccy={ccy}
          emphasis
          barClass={
            upliftPct >= 0
              ? "bg-gradient-to-r from-emerald-500/50 to-emerald-400/90"
              : "bg-gradient-to-r from-red-500/50 to-red-400/90"
          }
        />
      </div>

      {/* Price move, one line */}
      <div className="mt-5 flex items-baseline justify-between border-t border-white/[0.05] pt-3.5">
        <span className="q-label">{reval.commodity} price · {assumedLabel} → spot</span>
        <span className="font-mono text-[13px] tabular-nums">
          <span className="text-zinc-500">
            {fmtPriceForCommodity(reval.price_dfs, reval.commodity)}
          </span>
          <span className="mx-2 text-zinc-600">→</span>
          <span className={priceChangePct >= 0 ? "text-emerald-400" : "text-red-400"}>
            {fmtPriceForCommodity(reval.price_spot, reval.commodity)} {reval.price_unit}
          </span>
          <span
            className={`ml-2 text-[12px] ${
              priceChangePct >= 0 ? "text-emerald-400" : "text-red-400"
            }`}
          >
            {priceChangePct >= 0 ? "+" : ""}
            {priceChangePct.toFixed(0)}%
          </span>
        </span>
      </div>

      {/* Assumptions row */}
      <div className="mt-4 flex flex-wrap gap-1.5">
        {[
          `${reval.annual_production.toLocaleString()} ${reval.annual_production_unit}/yr`,
          `${reval.mine_life_years.toFixed(0)}y life`,
          `${reval.discount_rate_pct.toFixed(0)}% discount`,
          `${reval.tax_rate_pct.toFixed(0)}% tax${reval.warnings.some((w) => w.includes("defaulted")) ? " (default)" : ""}`,
          ...(reval.fx_rate != null ? [`FX ${reval.fx_rate.toFixed(4)}`] : []),
        ].map((t) => (
          <span
            key={t}
            className="rounded border border-white/[0.05] bg-white/[0.02] px-1.5 py-0.5 font-mono text-[10px] text-zinc-500 tabular-nums"
          >
            {t}
          </span>
        ))}
      </div>

      {/* Provenance */}
      <p className="mt-2.5 text-[10px] text-zinc-700">
        {fmtSpotSource(reval.spot_source)} &middot; {spotDate} &middot; {fmtMethodVersion(reval.method_version)}
      </p>
    </div>
  );
}

export function OperationsTab({ projects }: { projects: ProjectData[] }) {
  // Only show projects with a DFS/PFS study. JORC resource-only projects are
  // hidden (noisy, often mis-extracted names, not the focus here).
  const studyProjects = (projects ?? []).filter((p) => p.study);

  if (studyProjects.length === 0) {
    return <p className="text-sm text-zinc-500">No DFS/PFS studies on file.</p>;
  }

  return (
    <div className="space-y-6">
      {studyProjects.map((project, i) => (
        <div
          key={project.name}
          className="q-card q-card-hero animate-fade-up p-5"
          style={{ animationDelay: `${i * 60}ms` }}
        >
          <div className="flex items-center gap-3 mb-1">
            <h3 className="q-display text-[20px] text-zinc-100">{project.name}</h3>
            {stageBadge(project.stage)}
          </div>

          <p className="text-xs text-zinc-500 mb-3">
            {[
              project.commodities.join(", "),
              project.state,
              project.country !== "Australia" ? project.country : null,
            ]
              .filter(Boolean)
              .join(" · ")}
          </p>

          {project.study && (
            <StudyCard
              study={project.study}
              suppressCommodities={
                project.revaluation
                  ? new Set([project.revaluation.commodity])
                  : undefined
              }
            />
          )}

          {project.revaluation && project.revaluation.study_confidence_tier !== "conceptual" && (
            <RevaluationCard reval={project.revaluation} />
          )}
        </div>
      ))}
    </div>
  );
}
