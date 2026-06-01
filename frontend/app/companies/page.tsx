"use client";

import { Fragment, useEffect, useState, useCallback } from "react";
import {
  api,
  type PortfolioCompany,
  type PortfolioCompanyDetail,
  type PortfolioProject,
  type ProjectStage,
} from "@/lib/api";
import Link from "next/link";

// ─── Stage display ──────────────────────────────────────────────────

const STAGE_LABELS: Record<string, string> = {
  production: "Production",
  care_and_maintenance: "Care & Maint.",
  development: "Development",
  feasibility: "Feasibility",
  advanced_exploration: "Adv. Exploration",
  exploration: "Exploration",
  unknown: "Unknown",
};

const STAGE_COLORS: Record<string, string> = {
  production: "text-zinc-200",
  care_and_maintenance: "text-zinc-500",
  development: "text-amber",
  feasibility: "text-amber/80",
  advanced_exploration: "text-zinc-300",
  exploration: "text-zinc-500",
  unknown: "text-zinc-600 italic",
};

const STAGE_OPTIONS: ProjectStage[] = [
  "production",
  "care_and_maintenance",
  "development",
  "feasibility",
  "advanced_exploration",
  "exploration",
];

function StageBadge({ stage }: { stage: string | null }) {
  if (!stage) return <span className="text-zinc-700">—</span>;
  return (
    <span className={`text-[12px] ${STAGE_COLORS[stage] ?? "text-zinc-600"}`}>
      {STAGE_LABELS[stage] ?? stage}
    </span>
  );
}

// ─── Confidence dot ─────────────────────────────────────────────────

function ConfidenceDot({
  source,
  confidence,
  inferredAt,
}: {
  source: string | null;
  confidence: string | null;
  inferredAt: string | null;
}) {
  if (source !== "gemini_inferred") return null;

  const dotClass =
    confidence === "high"
      ? "bg-amber w-1.5 h-1.5 rounded-full inline-block"
      : confidence === "medium"
        ? "bg-zinc-400 w-1.5 h-1.5 rounded-full inline-block"
        : "border border-zinc-600 w-1.5 h-1.5 rounded-full inline-block";

  const tooltip = `Inferred by Gemini 2.5 Flash${inferredAt ? ` · ${new Date(inferredAt).toLocaleDateString()}` : ""}`;

  return <span className={dotClass} title={tooltip} />;
}

// ─── Smart site display ─────────────────────────────────────────────

function formatSites(c: PortfolioCompany): string {
  if (c.regions.length === 1 && c.states.length === 1) {
    return `${c.regions[0]}, ${c.states[0]}`;
  }
  if (c.countries.length === 1) {
    return c.states.join(", ") || c.countries[0];
  }
  return c.countries.join(", ");
}

// ─── Study formatting ───────────────────────────────────────────────

function formatStudy(stage: string | null, date: string | null): string {
  if (!stage || !date) return "—";
  const d = new Date(date);
  const month = d.toLocaleString("en", { month: "short" });
  const year = d.getFullYear();
  return `${stage} · ${month} ${year}`;
}

// ─── Expanded row ───────────────────────────────────────────────────

function ExpandedRow({ ticker }: { ticker: string }) {
  const [detail, setDetail] = useState<PortfolioCompanyDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .portfolioCompany(ticker)
      .then(setDetail)
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) {
    return (
      <tr>
        <td colSpan={9} className="px-6 py-4">
          <div className="animate-pulse space-y-2">
            {[1, 2].map((i) => (
              <div key={i} className="h-5 bg-zinc-800/30 rounded w-3/4" />
            ))}
          </div>
        </td>
      </tr>
    );
  }

  if (!detail || detail.projects.length === 0) {
    return (
      <tr>
        <td colSpan={9} className="px-6 py-4 text-zinc-600 text-[13px]">
          No project data available.
        </td>
      </tr>
    );
  }

  return (
    <>
      {detail.projects.map((p) => (
        <tr
          key={p.project_name}
          className="bg-zinc-900/40 border-b border-white/[0.03]"
        >
          <td className="px-3 py-1.5" />
          <td className="px-3 py-1.5 text-[12px] text-zinc-400 pl-8">
            {p.project_name}
          </td>
          <td className="px-3 py-1.5">
            <span className="inline-flex items-center gap-1.5">
              <StageBadge stage={p.stage} />
              <ConfidenceDot
                source={p.stage_source}
                confidence={p.stage_confidence}
                inferredAt={p.stage_inferred_at}
              />
            </span>
          </td>
          <td className="px-3 py-1.5 text-[12px] text-zinc-500">
            {[p.region, p.state, p.country].filter(Boolean).join(", ")}
          </td>
          <td className="px-3 py-1.5 text-[12px] text-zinc-500">
            {p.primary_commodity || "—"}
          </td>
          <td className="px-3 py-1.5 text-[12px] text-zinc-500">
            {p.latest_study
              ? formatStudy(
                  p.latest_study.study_stage,
                  p.latest_study.study_date
                )
              : "—"}
          </td>
          <td className="px-3 py-1.5 text-[12px] text-zinc-500">
            {p.latest_revaluation ? (
              <span>
                <span className={p.latest_revaluation.npv_uplift_pct > 1 ? "text-green-400" : p.latest_revaluation.npv_uplift_pct < -0.1 ? "text-red-400" : "text-zinc-400"}>
                  {p.latest_revaluation.npv_uplift_pct > 0 ? "+" : ""}{(p.latest_revaluation.npv_uplift_pct * 100).toFixed(0)}%
                </span>
                <span className="text-zinc-600 ml-1.5">
                  spot ${p.latest_revaluation.npv_spot.toFixed(0)}M
                </span>
              </span>
            ) : "—"}
          </td>
          <td className="px-3 py-1.5" />
        </tr>
      ))}
    </>
  );
}

// ─── Main page ──────────────────────────────────────────────────────

export default function CompaniesPage() {
  const [companies, setCompanies] = useState<PortfolioCompany[]>([]);
  const [totalCompanies, setTotalCompanies] = useState(0);
  const [asOf, setAsOf] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedTicker, setExpandedTicker] = useState<string | null>(null);

  // Filters
  const [country, setCountry] = useState("");
  const [commodity, setCommodity] = useState("");
  const [minStage, setMinStage] = useState("");
  const [singleProject, setSingleProject] = useState(false);
  const [studyAfter, setStudyAfter] = useState("");
  const [supportedOnly, setSupportedOnly] = useState(true);
  const [sort, setSort] = useState("most_advanced_stage_desc");

  const fetchData = useCallback(() => {
    setLoading(true);
    const filters: Record<string, string> = { sort };
    if (country) filters.country = country;
    if (commodity) filters.commodity = commodity;
    if (minStage) filters.min_stage = minStage;
    if (singleProject) filters.single_project_only = "true";
    if (studyAfter) filters.study_after = studyAfter;
    if (supportedOnly) filters.supported_only = "true";

    api
      .portfolioCompanies(filters)
      .then((data) => {
        setCompanies(data.companies);
        setTotalCompanies(data.total_companies);
        setAsOf(data.as_of);
      })
      .finally(() => setLoading(false));
  }, [country, commodity, minStage, singleProject, studyAfter, supportedOnly, sort]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Derive unique countries / commodities from data for dropdowns
  const allCountries = [
    ...new Set(companies.flatMap((c) => c.countries)),
  ].sort();
  const allCommodities = [
    ...new Set(companies.flatMap((c) => c.primary_commodities)),
  ].sort();

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-baseline justify-between">
        <div>
          <p className="text-xs uppercase tracking-wider text-zinc-500">
            Companies
          </p>
          <p className="text-sm text-zinc-400 mt-1">
            Project portfolios across {totalCompanies} ASX-listed mining issuers
          </p>
        </div>
        {asOf && (
          <p className="text-[11px] text-zinc-600">
            {new Date(asOf).toLocaleString()}
          </p>
        )}
      </div>

      {/* Filter row */}
      <div className="flex flex-wrap items-center gap-3">
        <select
          value={country}
          onChange={(e) => setCountry(e.target.value)}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600"
        >
          <option value="">All countries</option>
          {allCountries.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select
          value={commodity}
          onChange={(e) => setCommodity(e.target.value)}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600"
        >
          <option value="">All commodities</option>
          {allCommodities.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select
          value={minStage}
          onChange={(e) => setMinStage(e.target.value)}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600"
        >
          <option value="">Any stage</option>
          {STAGE_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {STAGE_LABELS[s]}
            </option>
          ))}
        </select>

        <label
          className="flex items-center gap-1.5 h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 cursor-pointer select-none"
          title="Show only Au/Ag/Cu companies that have a DFS or PFS study"
        >
          <input
            type="checkbox"
            checked={supportedOnly}
            onChange={(e) => setSupportedOnly(e.target.checked)}
            className="accent-zinc-400"
          />
          Au/Ag/Cu + DFS/PFS
        </label>

        <select
          value={singleProject ? "single" : "all"}
          onChange={(e) => setSingleProject(e.target.value === "single")}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600"
        >
          <option value="all">Any # projects</option>
          <option value="single">Single project only</option>
        </select>

        <label
          className="flex items-center gap-1.5 h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-500 focus-within:border-zinc-600"
          title="Show companies with a DFS/PFS dated on or after this date"
        >
          DFS/PFS after
          <input
            type="date"
            value={studyAfter}
            onChange={(e) => setStudyAfter(e.target.value)}
            className="bg-transparent text-zinc-300 focus:outline-none [color-scheme:dark]"
          />
          {studyAfter && (
            <button
              type="button"
              onClick={() => setStudyAfter("")}
              aria-label="Clear study date filter"
              className="text-zinc-500 hover:text-zinc-300"
            >
              ×
            </button>
          )}
        </label>

        <select
          value={sort}
          onChange={(e) => setSort(e.target.value)}
          className="h-8 rounded-sm border border-border bg-transparent px-2 text-[13px] text-zinc-300 focus:outline-none focus:border-zinc-600 ml-auto"
        >
          <option value="most_advanced_stage_desc">By stage</option>
          <option value="project_count">By project count</option>
          <option value="ticker">By ticker</option>
        </select>
      </div>

      {/* Result count */}
      <p className="text-[12px] text-zinc-500 text-right">
        {companies.length} companies match
      </p>

      {/* Loading */}
      {loading && (
        <div className="animate-pulse space-y-2">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <div key={i} className="h-8 bg-zinc-800/20 rounded" />
          ))}
        </div>
      )}

      {/* Table */}
      {!loading && (
        <div className="overflow-x-auto -mx-2">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left border-b border-border">
                {[
                  "Ticker",
                  "Company",
                  "Projects",
                  "Most advanced",
                  "Commodities",
                  "Sites",
                  "Latest study",
                  "Spot reval.",
                  "",
                ].map((h) => (
                  <th
                    key={h}
                    className="px-3 py-2 font-medium text-[11px] uppercase tracking-wider text-zinc-500"
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {companies.map((c) => (
                <Fragment key={c.ticker}>
                  <tr
                    className="hover:bg-white/[0.02] transition-colors cursor-pointer"
                    onClick={() =>
                      setExpandedTicker(
                        expandedTicker === c.ticker ? null : c.ticker
                      )
                    }
                  >
                    <td className="px-3 py-2">
                      <Link
                        href={`/company/${c.ticker}`}
                        onClick={(e) => e.stopPropagation()}
                        className="font-mono text-[13px] font-medium text-amber hover:text-amber/80 transition-colors"
                      >
                        {c.ticker}
                      </Link>
                    </td>
                    <td className="px-3 py-2 text-zinc-300 text-[13px]">
                      {c.company_name || "—"}
                    </td>
                    <td className="px-3 py-2 font-mono text-[13px] text-zinc-400">
                      <span className="inline-flex items-center gap-1.5">
                        {c.active_project_count}
                        {c.is_single_project && (
                          <span
                            className="w-1.5 h-1.5 rounded-full bg-amber inline-block"
                            title="Single project company"
                          />
                        )}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <StageBadge stage={c.most_advanced_stage} />
                    </td>
                    <td className="px-3 py-2 text-[13px] text-zinc-400">
                      {c.primary_commodities.length > 0
                        ? c.primary_commodities.join(" · ")
                        : "—"}
                    </td>
                    <td className="px-3 py-2 text-[13px] text-zinc-400">
                      {formatSites(c) || "—"}
                    </td>
                    <td className="px-3 py-2 text-[13px] text-zinc-500">
                      {formatStudy(
                        c.latest_study_stage,
                        c.latest_study_date
                      )}
                    </td>
                    <td className="px-3 py-2 text-[13px]">
                      {c.latest_revaluation ? (
                        <span className={c.latest_revaluation.npv_uplift_pct > 1 ? "text-green-400" : c.latest_revaluation.npv_uplift_pct < -0.1 ? "text-red-400" : "text-zinc-400"}>
                          {c.latest_revaluation.npv_uplift_pct > 0 ? "+" : ""}{(c.latest_revaluation.npv_uplift_pct * 100).toFixed(0)}%
                        </span>
                      ) : (
                        <span className="text-zinc-700">—</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-zinc-600 text-[13px]">
                      <span
                        className={`transition-transform inline-block ${expandedTicker === c.ticker ? "rotate-90" : ""}`}
                      >
                        ›
                      </span>
                    </td>
                  </tr>
                  {expandedTicker === c.ticker && (
                    <ExpandedRow
                      key={`${c.ticker}-expanded`}
                      ticker={c.ticker}
                    />
                  )}
                </Fragment>
              ))}
              {companies.length === 0 && (
                <tr>
                  <td
                    colSpan={9}
                    className="px-3 py-8 text-center text-zinc-600 text-[13px]"
                  >
                    No companies match these filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
