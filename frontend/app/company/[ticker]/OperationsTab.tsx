"use client";

import React from "react";
import type { ProjectData, ResourceRow, StudyData } from "@/types/snapshot";

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

function stageBadge(stage: string | null) {
  if (!stage) return null;
  const colors: Record<string, string> = {
    concept: "text-zinc-500 border-zinc-700",
    exploration: "text-zinc-500 border-zinc-700",
    discovery: "text-amber-400 border-amber-800",
    feasibility: "text-sky-400 border-sky-800",
    development: "text-violet-400 border-violet-800",
    production: "text-emerald-400 border-emerald-800",
  };
  const cls = colors[stage.toLowerCase()] ?? "text-zinc-400 border-zinc-700";
  return (
    <span className={`text-[10px] uppercase tracking-wider border px-1.5 py-0.5 rounded ${cls}`}>
      {stage}
    </span>
  );
}

const TOTAL_CATS = new Set(["Total", "Sub-total", "In-situ Total", "Stockpiles"]);

function splitBySections(resources: ResourceRow[], projectName: string) {
  // Group rows by section, keeping unsectioned rows as a separate "Total" group
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

  // If no sections detected, return all rows as one group
  if (sections.length === 0) {
    return [{ label: projectName, rows: resources }];
  }

  // Add unsectioned rows (In-situ Total, Stockpiles, Total) as a summary group
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
                <tr key={i} className={`hover:bg-white/[0.02] ${isTotal ? "border-t border-white/[0.06]" : ""}`}>
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

function StudyCard({ study }: { study: StudyData }) {
  const ccy = study.reporting_currency;
  const dr = study.discount_rate_pct != null ? study.discount_rate_pct.toFixed(0) : "?";

  return (
    <div className="mt-4">
      <div className="flex items-center gap-2 mb-3">
        <h4 className="text-xs font-medium text-zinc-400 uppercase tracking-wider">{study.study_type}</h4>
        {study.study_date && (
          <span className="text-[10px] text-zinc-600">{study.study_date}</span>
        )}
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
            <p className="text-sm text-zinc-200 font-medium">{study.aisc_per_unit.toFixed(0)} {study.aisc_unit ?? ""}</p>
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
          {study.price_assumptions.map((pa, i) => (
            <span key={i} className="text-[10px] text-zinc-500 border border-white/[0.06] rounded px-1.5 py-0.5">
              {pa.commodity} @ {pa.price.toLocaleString()} {pa.unit}
            </span>
          ))}
        </div>
      )}
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

          {project.study && <StudyCard study={project.study} />}

          <ResourceTable resources={project.resources} resourceDate={project.resource_date} projectName={project.name} />

          {!project.study && project.resources.length === 0 && (
            <p className="text-xs text-zinc-600 italic">No JORC estimate on file</p>
          )}
        </div>
      ))}
    </div>
  );
}
