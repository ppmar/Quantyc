"use client";

import {
  Fragment,
  Suspense,
  useEffect,
  useMemo,
  useState,
  useCallback,
} from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  api,
  type PortfolioCompany,
  type PortfolioCompanyDetail,
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

const SORT_LABELS: Record<string, string> = {
  most_advanced_stage_desc: "Stage",
  uplift_abs_desc: "Uplift A$",
  uplift_pct_desc: "Uplift %",
  project_count: "Projects",
  ticker: "Ticker",
};

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

// ─── Formatting helpers ─────────────────────────────────────────────

function formatSites(c: PortfolioCompany): string {
  if (c.regions.length === 1 && c.states.length === 1) {
    return `${c.regions[0]}, ${c.states[0]}`;
  }
  if (c.countries.length === 1) {
    return c.states.join(", ") || c.countries[0];
  }
  return c.countries.join(", ");
}

function formatStudy(stage: string | null, date: string | null): string {
  if (!stage) return "—";
  if (!date) return stage;
  const d = new Date(date);
  const month = d.toLocaleString("en", { month: "short" });
  return `${stage} · ${month} ${d.getFullYear()}`;
}

function UpliftCell({ pct, bar = false }: { pct: number; bar?: boolean }) {
  const cls =
    pct > 1 ? "text-green-400" : pct < -0.1 ? "text-red-400" : "text-zinc-400";
  // Heat strip: |pct| clamped to 150% maps to bar width.
  const width = Math.min(Math.abs(pct), 1.5) / 1.5;
  return (
    <span className="inline-flex items-center justify-end gap-2">
      {bar && (
        <span className="relative inline-block h-[5px] w-12 overflow-hidden rounded-full bg-white/[0.05]">
          <span
            className={`absolute inset-y-0 rounded-full ${
              pct >= 0 ? "right-0 bg-emerald-400/70" : "right-0 bg-red-400/70"
            }`}
            style={{ width: `${Math.max(width * 100, 4)}%` }}
          />
        </span>
      )}
      <span className={`font-mono ${cls}`}>
        {pct > 0 ? "+" : ""}
        {(pct * 100).toFixed(0)}%
      </span>
    </span>
  );
}

// ─── Filter controls ────────────────────────────────────────────────

function FilterSelect({
  value,
  onChange,
  children,
  ariaLabel,
}: {
  value: string;
  onChange: (v: string) => void;
  children: React.ReactNode;
  ariaLabel: string;
}) {
  return (
    <select
      aria-label={ariaLabel}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="q-control q-select"
    >
      {children}
    </select>
  );
}

function FilterToggle({
  on,
  onChange,
  title,
  children,
}: {
  on: boolean;
  onChange: (v: boolean) => void;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      className="q-toggle"
      data-on={on}
      title={title}
      onClick={() => onChange(!on)}
    >
      <span
        className={`inline-block w-1.5 h-1.5 rounded-full transition-colors ${
          on ? "bg-amber" : "border border-zinc-600"
        }`}
      />
      {children}
    </button>
  );
}

function Chip({
  label,
  onClear,
}: {
  label: string;
  onClear: () => void;
}) {
  return (
    <span className="q-chip">
      {label}
      <button type="button" onClick={onClear} aria-label={`Clear ${label}`}>
        ×
      </button>
    </span>
  );
}

// ─── Expanded row ───────────────────────────────────────────────────

const COLS = 9;

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
        <td colSpan={COLS} className="px-6 py-3">
          <div className="space-y-2">
            {[1, 2].map((i) => (
              <div key={i} className="h-5 rounded q-shimmer w-3/4" />
            ))}
          </div>
        </td>
      </tr>
    );
  }

  // Match the "Projects" count + Operations tab: only projects with a study.
  const studyProjects = (detail?.projects ?? []).filter((p) => p.latest_study);

  if (!detail || studyProjects.length === 0) {
    return (
      <tr className="animate-expand-in">
        <td colSpan={COLS} className="px-6 py-4 text-zinc-600 text-[13px]">
          No DFS/PFS study projects on file.
        </td>
      </tr>
    );
  }

  return (
    <>
      {studyProjects.map((p) => (
        <tr
          key={p.project_name}
          className="animate-expand-in bg-zinc-900/40 border-b border-white/[0.03]"
        >
          <td className="px-3 py-1.5 border-l-2 border-amber/30" />
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
              ? formatStudy(p.latest_study.study_stage, p.latest_study.study_date)
              : "—"}
          </td>
          <td className="px-3 py-1.5 text-[12px] text-right">
            {p.latest_revaluation ? (
              <span>
                <UpliftCell pct={p.latest_revaluation.npv_uplift_pct} />
                <span className="text-zinc-600 ml-1.5 font-mono">
                  spot ${p.latest_revaluation.npv_spot.toFixed(0)}M
                </span>
              </span>
            ) : (
              <span className="text-zinc-700">—</span>
            )}
          </td>
          <td className="px-3 py-1.5" colSpan={2} />
        </tr>
      ))}
    </>
  );
}

// ─── Main page ──────────────────────────────────────────────────────

const DEFAULT_SORT = "most_advanced_stage_desc";

function CompaniesPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [companies, setCompanies] = useState<PortfolioCompany[]>([]);
  const [asOf, setAsOf] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedTicker, setExpandedTicker] = useState<string | null>(null);

  // Filters — initialised from the URL so views are shareable/refreshable.
  const [query, setQuery] = useState(searchParams.get("q") ?? "");
  const [country, setCountry] = useState(searchParams.get("country") ?? "");
  const [commodity, setCommodity] = useState(
    searchParams.get("commodity") ?? ""
  );
  const [minStage, setMinStage] = useState(searchParams.get("stage") ?? "");
  const [singleProject, setSingleProject] = useState(
    searchParams.get("single") === "1"
  );
  const [studyAfter, setStudyAfter] = useState(
    searchParams.get("after") ?? ""
  );
  const [supportedOnly, setSupportedOnly] = useState(
    searchParams.get("all") !== "1"
  );
  const [hideEmpty, setHideEmpty] = useState(searchParams.get("ne") === "1");
  const [sort, setSort] = useState(searchParams.get("sort") ?? DEFAULT_SORT);

  // Reflect filter state into the URL (replace, no history spam).
  useEffect(() => {
    const p = new URLSearchParams();
    if (query) p.set("q", query);
    if (country) p.set("country", country);
    if (commodity) p.set("commodity", commodity);
    if (minStage) p.set("stage", minStage);
    if (singleProject) p.set("single", "1");
    if (studyAfter) p.set("after", studyAfter);
    if (!supportedOnly) p.set("all", "1");
    if (hideEmpty) p.set("ne", "1");
    if (sort !== DEFAULT_SORT) p.set("sort", sort);
    const qs = p.toString();
    router.replace(qs ? `/companies?${qs}` : "/companies", { scroll: false });
  }, [query, country, commodity, minStage, singleProject, studyAfter, supportedOnly, hideEmpty, sort, router]);

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
        setAsOf(data.as_of);
      })
      .finally(() => setLoading(false));
  }, [country, commodity, minStage, singleProject, studyAfter, supportedOnly, sort]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Client-side display filters: free-text search + optional 0-project hide.
  const visibleCompanies = useMemo(() => {
    let list = companies;
    if (hideEmpty) list = list.filter((c) => c.study_project_count > 0);
    const q = query.trim().toLowerCase();
    if (q) {
      list = list.filter(
        (c) =>
          c.ticker.toLowerCase().includes(q) ||
          (c.company_name ?? "").toLowerCase().includes(q)
      );
    }
    return list;
  }, [companies, hideEmpty, query]);

  // Derive unique countries / commodities from data for dropdowns
  const allCountries = useMemo(
    () => [...new Set(companies.flatMap((c) => c.countries))].sort(),
    [companies]
  );
  const allCommodities = useMemo(
    () => [...new Set(companies.flatMap((c) => c.primary_commodities))].sort(),
    [companies]
  );

  // Active filter chips (everything except sort + default supportedOnly)
  const chips: { label: string; clear: () => void }[] = [];
  if (query) chips.push({ label: `“${query}”`, clear: () => setQuery("") });
  if (country) chips.push({ label: country, clear: () => setCountry("") });
  if (commodity) chips.push({ label: commodity, clear: () => setCommodity("") });
  if (minStage)
    chips.push({
      label: `≥ ${STAGE_LABELS[minStage]}`,
      clear: () => setMinStage(""),
    });
  if (singleProject)
    chips.push({ label: "Single project", clear: () => setSingleProject(false) });
  if (studyAfter)
    chips.push({
      label: `Study ≥ ${studyAfter}`,
      clear: () => setStudyAfter(""),
    });
  if (hideEmpty)
    chips.push({ label: "Hide 0-project", clear: () => setHideEmpty(false) });
  if (!supportedOnly)
    chips.push({
      label: "All commodities/stages",
      clear: () => setSupportedOnly(true),
    });

  const clearAll = () => {
    setQuery("");
    setCountry("");
    setCommodity("");
    setMinStage("");
    setSingleProject(false);
    setStudyAfter("");
    setSupportedOnly(true);
    setHideEmpty(false);
  };

  return (
    <div className="space-y-5 animate-fade-up">
      {/* Header */}
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="q-display text-[28px] leading-none text-zinc-100">
            Companies
          </h1>
          <p className="mt-1.5 text-[13px] text-zinc-500">
            Project portfolios across{" "}
            <span className="font-mono text-zinc-300">
              {visibleCompanies.length}
            </span>{" "}
            ASX-listed mining issuers
          </p>
        </div>
        {asOf && (
          <p className="text-[11px] text-zinc-600 font-mono">
            as of {new Date(asOf).toLocaleString()}
          </p>
        )}
      </div>

      {/* Toolbar */}
      <div className="q-card q-card-hero px-3.5 py-3 space-y-2.5">
        <div className="flex flex-wrap items-center gap-2">
          {/* Search */}
          <label className="q-control flex items-center gap-2 w-52">
            <svg
              width="13"
              height="13"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              className="text-zinc-600 shrink-0"
            >
              <circle cx="11" cy="11" r="7" />
              <path d="m21 21-4.3-4.3" />
            </svg>
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Ticker or name…"
              className="bg-transparent w-full text-[13px] text-zinc-200 placeholder:text-zinc-600 focus:outline-none"
            />
            {query && (
              <button
                type="button"
                onClick={() => setQuery("")}
                aria-label="Clear search"
                className="text-zinc-600 hover:text-zinc-300"
              >
                ×
              </button>
            )}
          </label>

          <FilterSelect ariaLabel="Country" value={country} onChange={setCountry}>
            <option value="">All countries</option>
            {allCountries.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </FilterSelect>

          <FilterSelect
            ariaLabel="Commodity"
            value={commodity}
            onChange={setCommodity}
          >
            <option value="">All commodities</option>
            {allCommodities.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </FilterSelect>

          <FilterSelect ariaLabel="Minimum stage" value={minStage} onChange={setMinStage}>
            <option value="">Any stage</option>
            {STAGE_OPTIONS.map((s) => (
              <option key={s} value={s}>
                ≥ {STAGE_LABELS[s]}
              </option>
            ))}
          </FilterSelect>

          <FilterSelect
            ariaLabel="Project count"
            value={singleProject ? "single" : "all"}
            onChange={(v) => setSingleProject(v === "single")}
          >
            <option value="all">Any # projects</option>
            <option value="single">Single project only</option>
          </FilterSelect>

          <label
            className="q-control flex items-center gap-1.5 text-zinc-500"
            title="Show companies with a DFS/PFS dated on or after this date"
          >
            Study ≥
            <input
              type="date"
              value={studyAfter}
              onChange={(e) => setStudyAfter(e.target.value)}
              className="bg-transparent text-zinc-300 focus:outline-none [color-scheme:dark] text-[12px]"
            />
          </label>

          <FilterToggle
            on={supportedOnly}
            onChange={setSupportedOnly}
            title="Show only Au/Ag/Cu companies that have a DFS or PFS study"
          >
            Au/Ag/Cu + study
          </FilterToggle>

          <FilterToggle
            on={hideEmpty}
            onChange={setHideEmpty}
            title="Hide companies with no DFS/PFS study projects"
          >
            Hide 0-project
          </FilterToggle>

          {/* Sort — right aligned */}
          <div className="ml-auto flex items-center gap-1.5">
            <span className="text-[11px] uppercase tracking-wider text-zinc-600">
              Sort
            </span>
            <FilterSelect ariaLabel="Sort" value={sort} onChange={setSort}>
              {Object.entries(SORT_LABELS).map(([k, v]) => (
                <option key={k} value={k}>
                  {v}
                </option>
              ))}
            </FilterSelect>
          </div>
        </div>

        {/* Active chips */}
        {chips.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5 pt-0.5 border-t border-white/[0.04]">
            <span className="text-[11px] text-zinc-600 mr-1 pt-0.5">
              {visibleCompanies.length} match
            </span>
            {chips.map((c) => (
              <Chip key={c.label} label={c.label} onClear={c.clear} />
            ))}
            <button
              type="button"
              onClick={clearAll}
              className="text-[12px] text-zinc-500 hover:text-zinc-300 transition-colors ml-1"
            >
              Clear all
            </button>
          </div>
        )}
      </div>

      {/* Loading */}
      {loading && (
        <div className="space-y-1.5 pt-2">
          {[1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
            <div key={i} className="h-8 rounded q-shimmer" />
          ))}
        </div>
      )}

      {/* Table */}
      {!loading && (
        <div className="q-card overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-12 lg:top-0 z-10 bg-[oklch(0.115_0.005_260/0.97)] backdrop-blur-sm">
              <tr className="text-left border-b border-border-strong">
                {(
                  [
                    ["Ticker", "", "ticker"],
                    ["Company", "", null],
                    ["Projects", "text-right", "project_count"],
                    ["Most advanced", "", "most_advanced_stage_desc"],
                    ["Commodities", "", null],
                    ["Sites", "", null],
                    ["Latest study", "", null],
                    // Click cycles uplift % ↔ uplift A$.
                    ["Spot reval.", "text-right", "uplift_pct_desc"],
                    ["", "", null],
                  ] as [string, string, string | null][]
                ).map(([h, align, key], i) => {
                  const isActive =
                    key !== null &&
                    (sort === key ||
                      (key === "uplift_pct_desc" && sort === "uplift_abs_desc"));
                  return (
                    <th
                      key={i}
                      onClick={
                        key
                          ? () =>
                              setSort(
                                key === "uplift_pct_desc" &&
                                  sort === "uplift_pct_desc"
                                  ? "uplift_abs_desc"
                                  : key
                              )
                          : undefined
                      }
                      title={
                        key === "uplift_pct_desc"
                          ? "Click to sort by uplift % / A$"
                          : key
                            ? "Click to sort"
                            : undefined
                      }
                      className={`px-3 py-2 font-medium text-[11px] uppercase tracking-wider transition-colors ${align} ${
                        key ? "cursor-pointer select-none" : ""
                      } ${
                        isActive
                          ? "text-amber"
                          : key
                            ? "text-zinc-500 hover:text-zinc-300"
                            : "text-zinc-500"
                      }`}
                    >
                      {h}
                      {isActive && (
                        <span className="ml-1">
                          {sort === "uplift_abs_desc" ? "A$▾" : "▾"}
                        </span>
                      )}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {visibleCompanies.map((c, idx) => (
                <Fragment key={c.ticker}>
                  <tr
                    className={`group hover:bg-white/[0.025] transition-colors cursor-pointer animate-fade-up ${
                      expandedTicker === c.ticker ? "bg-white/[0.02]" : ""
                    }`}
                    style={{ animationDelay: `${Math.min(idx, 20) * 18}ms` }}
                    onClick={() =>
                      setExpandedTicker(
                        expandedTicker === c.ticker ? null : c.ticker
                      )
                    }
                  >
                    <td className="px-3 py-2 border-l-2 border-transparent group-hover:border-amber/50 transition-colors">
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
                    <td className="px-3 py-2 font-mono text-[13px] text-zinc-400 text-right">
                      <span className="inline-flex items-center gap-1.5">
                        {c.is_single_project && (
                          <span
                            className="w-1.5 h-1.5 rounded-full bg-amber inline-block"
                            title="Single project company"
                          />
                        )}
                        {c.study_project_count}
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
                      {formatStudy(c.latest_study_stage, c.latest_study_date)}
                    </td>
                    <td className="px-3 py-2 text-[13px] text-right">
                      {c.latest_revaluation ? (
                        <UpliftCell
                          pct={c.latest_revaluation.npv_uplift_pct}
                          bar
                        />
                      ) : (
                        <span className="text-zinc-700">—</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-zinc-600 text-[13px] w-8">
                      <span
                        className={`transition-transform inline-block ${
                          expandedTicker === c.ticker ? "rotate-90" : ""
                        }`}
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
              {visibleCompanies.length === 0 && (
                <tr>
                  <td colSpan={COLS} className="px-3 py-14 text-center">
                    <p className="text-zinc-500 text-[13px]">
                      No companies match these filters.
                    </p>
                    <button
                      type="button"
                      onClick={clearAll}
                      className="mt-2 text-[12px] text-amber/80 hover:text-amber transition-colors"
                    >
                      Clear all filters
                    </button>
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

export default function CompaniesPage() {
  return (
    <Suspense
      fallback={
        <div className="space-y-1.5 pt-2">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <div key={i} className="h-8 q-shimmer" />
          ))}
        </div>
      }
    >
      <CompaniesPageInner />
    </Suspense>
  );
}
