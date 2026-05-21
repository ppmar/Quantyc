"use client";

import React from "react";
import type { ProjectData, ResourceRow, StudyData, RevaluationData } from "@/types/snapshot";

function fmtTonnes(val: number | null) {
  if (val == null) return "—";
  if (val >= 1000) return `${(val / 1000).toFixed(1)}Bt`;
  if (val >= 1) return `${val.toFixed(1)}Mt`;
  return `${(val * 1000).toFixed(0)}Kt`;
}

function fmtGrade(val: number | null, unit: string | null) {
  if (val == null) return "—";
  return `${val.toFixed(2)} ${unit ?? ""}`.trim();
}

function fmtContained(val: number | null, unit: string | null) {
  if (val == null) return "—";
  if (val >= 1000) return `${(val / 1000).toFixed(2)} M${(unit ?? "").replace(/^k/i, "")}`;
  return `${val.toFixed(0)} ${unit ?? ""}`.trim();
}

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

const TOTAL_CATS = new Set(["Total", "Sub-total", "In-situ Total", "Stockpiles"]);

function splitBySections(resources: ResourceRow[], projectName: string) {
  const sections: { label: string; rows: ResourceRow[] }[] = [];
  const unsectioned: ResourceRow[] = [];
  const sectionMap = new Map<string, ResourceRow[]>();

  for (const r of resources) {
    if (r.section) {
      if (!sectionMap.has(r.section)) {
        const arr: ResourceRow[] = [];
        sectionMap.set(r.section, arr);
        sections.push({ label: `${projectName} — ${r.section}`, rows: arr });
      }
      sectionMap.get(r.section)!.push(r);
    } else {
      unsectioned.push(r);
    }
  }

  if (sections.length === 0) {
    return [{ label: projectName, rows: resources }];
  }

  if (unsectioned.length > 0) {
    sections.push({ label: `${projectName} — Total`, rows: unsectioned });
  }

  return sections;
}

function SectionTable({ label, rows }: { label: string; rows: ResourceRow[] }) {
  return (
    <div className="mt-3">
      <h4 className="text-xs font-medium text-zinc-400 uppercase tracking-wider mb-2 px-1">{label}</h4>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-zinc-500 border-b border-white/[0.06]">
              <th className="px-3 py-2 font-medium text-xs uppercase tracking-wider">Category</th>
              <th className="px-3 py-2 font-medium text-xs uppercase tracking-wider text-right">Tonnes</th>
              <th className="px-3 py-2 font-medium text-xs uppercase tracking-wider text-right">Grade</th>
              <th className="px-3 py-2 font-medium text-xs uppercase tracking-wider text-right">Contained</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const isTotal = TOTAL_CATS.has(r.category);
              const textCls = isTotal ? "text-zinc-200 font-medium" : "text-zinc-400";
              const valCls = isTotal ? "text-zinc-200 font-medium" : "text-zinc-300";
              return (
                <tr key={i} className={`${isTotal ? "border-t border-white/[0.06]" : ""}`}>
                  <td className={`px-3 py-1.5 text-xs ${textCls}`}>{r.category}</td>
                  <td className={`px-3 py-1.5 text-xs ${valCls} text-right`}>{fmtTonnes(r.tonnes_mt)}</td>
                  <td className={`px-3 py-1.5 text-xs ${valCls} text-right`}>{fmtGrade(r.grade, r.grade_unit)}</td>
                  <td className={`px-3 py-1.5 text-xs ${valCls} text-right`}>{fmtContained(r.contained_metal, r.contained_metal_unit)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ResourceTable({ resources, resourceDate, projectName }: { resources: ResourceRow[]; resourceDate: string | null; projectName: string }) {
  if (resources.length === 0) return null;

  const sections = splitBySections(resources, projectName);

  return (
    <div>
      {sections.map((s, i) => (
        <SectionTable key={i} label={s.label} rows={s.rows} />
      ))}
      {resourceDate && (
        <p className="text-[10px] text-zinc-600 mt-2 px-3">Estimate as at {resourceDate}</p>
      )}
    </div>
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

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {study.post_tax_npv != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">NPV{dr} post-tax</p>
            <p className="text-sm text-zinc-200 font-medium">{fmtMoney(study.post_tax_npv, ccy)}</p>
          </div>
        )}
        {study.pre_tax_npv != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">NPV{dr} pre-tax</p>
            <p className="text-sm text-zinc-200 font-medium">{fmtMoney(study.pre_tax_npv, ccy)}</p>
          </div>
        )}
        {study.irr_pct != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">IRR</p>
            <p className="text-sm text-zinc-200 font-medium">{fmtPct(study.irr_pct)}</p>
          </div>
        )}
        {study.initial_capex != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Initial capex</p>
            <p className="text-sm text-zinc-200 font-medium">{fmtMoney(study.initial_capex, ccy)}</p>
          </div>
        )}
        {study.aisc_per_unit != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">AISC</p>
            <p className="text-sm text-zinc-200 font-medium">{fmtPerUnitUSD(study.aisc_per_unit, study.aisc_unit)}</p>
          </div>
        )}
        {study.mine_life_years != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Mine life</p>
            <p className="text-sm text-zinc-200 font-medium">{study.mine_life_years.toFixed(0)} years</p>
          </div>
        )}
        {study.payback_years != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Payback</p>
            <p className="text-sm text-zinc-200 font-medium">{study.payback_years.toFixed(1)} years</p>
          </div>
        )}
        {study.recovery_pct != null && (
          <div>
            <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Recovery</p>
            <p className="text-sm text-zinc-200 font-medium">{fmtPct(study.recovery_pct)}</p>
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

function signalBorderColor(upliftPct: number) {
  if (upliftPct > 0.5) return "border-emerald-800";
  if (upliftPct > 0.15) return "border-emerald-900";
  if (upliftPct > 0) return "border-white/[0.06]";
  if (upliftPct > -0.15) return "border-white/[0.06]";
  if (upliftPct > -0.5) return "border-amber-900";
  return "border-red-900";
}

function RevaluationCard({ reval }: { reval: RevaluationData }) {
  const ccy = reval.reporting_currency === "USD" ? "US$" : reval.reporting_currency === "AUD" ? "A$" : `${reval.reporting_currency ?? ""}$`;
  const upliftPct = reval.npv_uplift_pct;
  const priceChangePct = ((reval.price_spot - reval.price_dfs) / reval.price_dfs) * 100;

  const spotDate = reval.spot_fetched_at
    ? new Date(reval.spot_fetched_at).toLocaleDateString("en-AU", { day: "numeric", month: "short", year: "numeric" })
    : "";

  return (
    <div className={`mt-4 border rounded-lg p-4 ${signalBorderColor(upliftPct)} bg-white/[0.02]`}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-1.5">
          <h4 className="text-xs font-medium text-zinc-400 uppercase tracking-wider">
            Spot revaluation
          </h4>
          <MethodologyHint />
        </div>
        <span className={`text-lg font-semibold tabular-nums ${signalColor(upliftPct)}`}>
          {upliftPct >= 0 ? "+" : ""}{(upliftPct * 100).toFixed(0)}%
        </span>
      </div>

      {/* Price comparison */}
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div>
          <p className="text-[10px] text-zinc-500 uppercase tracking-wider">DFS assumed</p>
          <p className="text-sm text-zinc-400 tabular-nums">
            {fmtPriceForCommodity(reval.price_dfs, reval.commodity)} {reval.price_unit}
          </p>
        </div>
        <div>
          <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Spot price</p>
          <p className="text-sm text-zinc-200 font-medium tabular-nums">
            {fmtPriceForCommodity(reval.price_spot, reval.commodity)} {reval.price_unit}
            <span className={`ml-1.5 text-xs ${priceChangePct >= 0 ? "text-emerald-500" : "text-red-400"}`}>
              {priceChangePct >= 0 ? "+" : ""}{priceChangePct.toFixed(0)}%
            </span>
          </p>
        </div>
      </div>

      {/* NPV comparison */}
      <div className="grid grid-cols-3 gap-3 py-3 border-t border-white/[0.06]">
        <div>
          <p className="text-[10px] text-zinc-500 uppercase tracking-wider">NPV (DFS)</p>
          <p className="text-sm text-zinc-400 tabular-nums">{ccy}{reval.npv_dfs.toLocaleString(undefined, { maximumFractionDigits: 0 })}M</p>
        </div>
        <div>
          <p className="text-[10px] text-zinc-500 uppercase tracking-wider">NPV (at spot)</p>
          <p className="text-sm text-zinc-200 font-medium tabular-nums">{ccy}{reval.npv_spot.toLocaleString(undefined, { maximumFractionDigits: 0 })}M</p>
        </div>
        <div>
          <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Uplift</p>
          <p className={`text-sm font-medium tabular-nums ${signalColor(upliftPct)}`}>
            {reval.npv_uplift >= 0 ? "+" : ""}{ccy}{reval.npv_uplift.toLocaleString(undefined, { maximumFractionDigits: 0 })}M
          </p>
        </div>
      </div>

      {/* Assumptions row */}
      <div className="flex flex-wrap gap-x-4 gap-y-1 mt-3">
        <span className="text-[10px] text-zinc-600 tabular-nums">
          {reval.annual_production.toLocaleString()} {reval.annual_production_unit}/yr
        </span>
        <span className="text-[10px] text-zinc-600 tabular-nums">
          {reval.mine_life_years.toFixed(0)}yr mine life
        </span>
        <span className="text-[10px] text-zinc-600 tabular-nums">
          {reval.discount_rate_pct.toFixed(0)}% discount
        </span>
        <span className="text-[10px] text-zinc-600 tabular-nums">
          {reval.tax_rate_pct.toFixed(0)}% tax{reval.warnings.some(w => w.includes("defaulted")) ? " (default)" : ""}
        </span>
        {reval.fx_rate != null && (
          <span className="text-[10px] text-zinc-600 tabular-nums">
            FX {reval.fx_rate.toFixed(4)}
          </span>
        )}
      </div>

      {/* Provenance */}
      <p className="text-[10px] text-zinc-700 mt-2">
        {fmtSpotSource(reval.spot_source)} &middot; {spotDate} &middot; {fmtMethodVersion(reval.method_version)}
      </p>
    </div>
  );
}

export function OperationsTab({ projects }: { projects: ProjectData[] }) {
  if (!projects || projects.length === 0) {
    return <p className="text-sm text-zinc-500">No project data yet.</p>;
  }

  return (
    <div className="space-y-8">
      {projects.map((project) => (
        <div key={project.name} className="border border-white/[0.06] rounded-lg p-4">
          <div className="flex items-center gap-3 mb-1">
            <h3 className="text-sm font-medium text-zinc-200">{project.name}</h3>
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

          {project.revaluation && <RevaluationCard reval={project.revaluation} />}

          <ResourceTable resources={project.resources} resourceDate={project.resource_date} projectName={project.name} />

          {!project.study && project.resources.length === 0 && (
            <p className="text-xs text-zinc-600 italic">No JORC estimate on file</p>
          )}
        </div>
      ))}
    </div>
  );
}
