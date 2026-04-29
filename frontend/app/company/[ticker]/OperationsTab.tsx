"use client";

import type { ProjectData, ResourceRow } from "@/types/snapshot";

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

function ResourceTable({ resources, resourceDate }: { resources: ResourceRow[]; resourceDate: string | null }) {
  if (resources.length === 0) return null;

  const resourceRows = resources.filter((r) => r.type === "resource");
  const reserveRows = resources.filter((r) => r.type === "reserve");

  const renderSection = (rows: ResourceRow[], label: string) => {
    if (rows.length === 0) return null;
    return (
      <>
        <tr>
          <td colSpan={4} className="px-3 pt-3 pb-1 text-[10px] uppercase tracking-wider text-zinc-500 font-medium">
            {label}
          </td>
        </tr>
        {rows.map((r, i) => (
          <tr key={`${label}-${i}`} className="hover:bg-white/[0.02]">
            <td className="px-3 py-1.5 text-xs text-zinc-400">{r.category}</td>
            <td className="px-3 py-1.5 text-xs text-zinc-300 text-right">{fmtTonnes(r.tonnes_mt)}</td>
            <td className="px-3 py-1.5 text-xs text-zinc-300 text-right">{fmtGrade(r.grade, r.grade_unit)}</td>
            <td className="px-3 py-1.5 text-xs text-zinc-300 text-right">{fmtContained(r.contained_metal, r.contained_metal_unit)}</td>
          </tr>
        ))}
      </>
    );
  };

  return (
    <div className="mt-3">
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
          <tbody className="divide-y divide-white/[0.04]">
            {renderSection(resourceRows, "Mineral Resources")}
            {renderSection(reserveRows, "Ore Reserves")}
          </tbody>
        </table>
      </div>
      {resourceDate && (
        <p className="text-[10px] text-zinc-600 mt-2 px-3">Estimate as at {resourceDate}</p>
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

          <ResourceTable resources={project.resources} resourceDate={project.resource_date} />

          {project.resources.length === 0 && (
            <p className="text-xs text-zinc-600 italic">No JORC estimate on file</p>
          )}
        </div>
      ))}
    </div>
  );
}
