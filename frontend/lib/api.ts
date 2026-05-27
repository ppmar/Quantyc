const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

async function fetchAPI<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export interface Stats {
  companies: number;
  documents: number;
  docs_parsed: number;
  docs_pending: number;
  docs_classified: number;
  docs_failed: number;
  financials: number;
  needs_review: number;
}

export interface Company {
  ticker: string;
  name: string | null;
  reporting_currency: string;
  doc_count: number;
  parsed_count: number;
}

export interface Document {
  document_id: number;
  ticker: string;
  doc_type: string;
  header: string | null;
  announcement_date: string | null;
  url: string;
  parse_status: string;
  parse_error: string | null;
  ingested_at: string;
}

export interface CompanyFinancial {
  financial_id: number;
  company_id: number;
  document_id: number;
  effective_date: string;
  announcement_date: string;
  shares_basic: number | null;
  shares_fd: number | null;
  options_outstanding: number | null;
  perf_rights_outstanding: number | null;
  convertibles_face_value: number | null;
  cash: number | null;
  debt: number | null;
  quarterly_opex_burn: number | null;
  quarterly_invest_burn: number | null;
  extraction_method: string;
  confidence: string;
  needs_review: number;
  review_reason: string | null;
  reviewed_at: string | null;
  created_at: string;
}

export interface FinancialsResponse {
  ticker: string;
  latest: CompanyFinancial & { name: string | null; reporting_currency: string };
  history: CompanyFinancial[];
}

export interface ReviewItem {
  financial_id: number;
  ticker: string;
  effective_date: string;
  shares_basic: number | null;
  shares_fd: number | null;
  cash: number | null;
  debt: number | null;
  quarterly_opex_burn: number | null;
  extraction_method: string;
  confidence: string;
  review_reason: string | null;
  url: string;
  header: string | null;
}

export interface PipelineStatus {
  running: boolean;
  ticker: string | null;
  phase: string | null;
  current_doc: string | null;
  docs_total: number;
  docs_done: number;
  started_at: number | null;
  error: string | null;
  failed_count: number;
}

// ─── Portfolio types ────────────────────────────────────────────────

export type ProjectStage =
  | "production"
  | "care_and_maintenance"
  | "development"
  | "feasibility"
  | "advanced_exploration"
  | "exploration"
  | "unknown";

export interface PortfolioCompany {
  ticker: string;
  company_name: string | null;
  active_project_count: number;
  total_project_count: number;
  is_single_project: boolean;
  most_advanced_stage: ProjectStage | null;
  stage_breakdown: Record<string, number>;
  primary_commodities: string[];
  countries: string[];
  states: string[];
  regions: string[];
  has_recent_study: boolean;
  latest_study_date: string | null;
  latest_study_stage: string | null;
  latest_revaluation: {
    npv_dfs: number;
    npv_spot: number;
    npv_uplift_pct: number;
    commodity: string;
    price_spot: number;
  } | null;
}

export interface PortfolioCompaniesResponse {
  filters_applied: Record<string, unknown>;
  as_of: string;
  total_companies: number;
  companies: PortfolioCompany[];
}

export interface PortfolioProject {
  project_name: string;
  stage: ProjectStage | null;
  stage_source:
    | "ozmin"
    | "gemini_inferred"
    | "insufficient_evidence"
    | "minedex"
    | "manual"
    | null;
  stage_confidence: "high" | "medium" | "low" | null;
  stage_inferred_at: string | null;
  country: string | null;
  state: string | null;
  region: string | null;
  primary_commodity: string | null;
  all_commodities: string[];
  ownership_pct: number | null;
  is_active: boolean;
  latest_study: {
    study_stage: string;
    study_date: string | null;
    study_confidence_tier: string | null;
    post_tax_npv: number | null;
    reporting_currency: string;
  } | null;
  latest_resource: {
    commodity: string;
    category: string;
    tonnes: number | null;
    grade: number | null;
    grade_unit: string | null;
    contained_metal: number | null;
    contained_metal_unit: string | null;
    effective_date: string | null;
  } | null;
  latest_revaluation: {
    commodity: string;
    price_dfs: number;
    price_spot: number;
    npv_dfs: number;
    npv_spot: number;
    npv_uplift_pct: number;
    computed_at: string;
    method_version: string | null;
    study_confidence_tier: string | null;
  } | null;
  document_counts: {
    studies: number;
    resources: number;
    all_documents: number;
  };
}

export interface PortfolioCompanyDetail {
  ticker: string;
  company_name: string | null;
  as_of: string;
  projects: PortfolioProject[];
}

export const api = {
  stats: () => fetchAPI<Stats>("/api/stats"),
  companies: () => fetchAPI<Company[]>("/api/companies"),
  documents: (params?: { status?: string; type?: string; ticker?: string }) => {
    const search = new URLSearchParams();
    if (params?.status) search.set("status", params.status);
    if (params?.type) search.set("type", params.type);
    if (params?.ticker) search.set("ticker", params.ticker);
    const qs = search.toString();
    return fetchAPI<Document[]>(`/api/documents${qs ? `?${qs}` : ""}`);
  },
  financials: (ticker: string) =>
    fetchAPI<FinancialsResponse>(`/api/companies/${ticker}/financials`),
  review: () => fetchAPI<ReviewItem[]>("/api/review"),
  pipelineStatus: () => fetchAPI<PipelineStatus>("/api/pipeline/status"),
  ingest: (tickers: string[], count: number = 20) =>
    fetch(`${API_BASE}/api/ingest`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tickers, count }),
    }).then((r) => r.json()),
  orchestrate: () =>
    fetch(`${API_BASE}/api/orchestrate`, { method: "POST" }).then((r) =>
      r.json()
    ),
  snapshot: (ticker: string) =>
    fetchAPI<import("@/types/snapshot").CompanySnapshot>(
      `/api/company/${ticker.toUpperCase()}/snapshot`
    ),
  portfolioCompanies: (filters?: Record<string, string>) => {
    const search = new URLSearchParams();
    if (filters) {
      for (const [k, v] of Object.entries(filters)) {
        if (v) search.set(k, v);
      }
    }
    const qs = search.toString();
    return fetchAPI<PortfolioCompaniesResponse>(
      `/api/portfolio/companies${qs ? `?${qs}` : ""}`
    );
  },
  portfolioCompany: (ticker: string) =>
    fetchAPI<PortfolioCompanyDetail>(
      `/api/portfolio/companies/${ticker.toUpperCase()}`
    ),
  upload: (ticker: string, files: FileList) => {
    const form = new FormData();
    form.set("ticker", ticker);
    Array.from(files).forEach((f) => form.append("files", f));
    return fetch(`${API_BASE}/api/upload`, { method: "POST", body: form }).then(
      (r) => r.json()
    );
  },
};
