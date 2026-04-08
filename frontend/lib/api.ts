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
  docs_done: number;
  docs_pending: number;
  docs_failed: number;
  staging_rows: number;
  needs_review: number;
  resources: number;
  financials: number;
  studies: number;
  drill_intercepts: number;
  drill_holes: number;
}

export interface Company {
  ticker: string;
  name: string | null;
  primary_commodity: string | null;
  doc_count: number;
  parsed_count: number;
  stage: string | null;
  cash: number | null;
  runway: number | null;
}

export interface Document {
  id: string;
  company_ticker: string;
  doc_type: string;
  header: string | null;
  announcement_date: string | null;
  url: string;
  local_path: string;
  parse_status: string;
  created_at: string;
}

export interface Resource {
  id: number;
  project_id: string;
  commodity: string;
  effective_date: string | null;
  estimate_type: string;
  category: string;
  tonnes_mt: number | null;
  grade: number | null;
  grade_unit: string | null;
  contained_metal: number | null;
  contained_unit: string | null;
  attributable_contained: number | null;
  project_name: string | null;
}

export interface Study {
  id: number;
  project_id: string;
  study_stage: string | null;
  study_date: string | null;
  mine_life_years: number | null;
  annual_production: number | null;
  production_unit: string | null;
  recovery_pct: number | null;
  initial_capex_musd: number | null;
  sustaining_capex_musd: number | null;
  opex_per_unit: number | null;
  opex_unit: string | null;
  post_tax_npv_musd: number | null;
  irr_pct: number | null;
  assumed_commodity_price: number | null;
  assumed_price_unit: string | null;
  project_name: string | null;
}

export interface DrillResult {
  hole_id: string;
  from_m: number | null;
  to_m: number | null;
  interval_m: number | null;
  au_gt: number | null;
  au_eq_gt: number | null;
  sb_pct: number | null;
  is_including: boolean;
}

export interface Valuation {
  ticker: string;
  stage: string;
  method: string;
  ev_aud: number | null;
  nav_aud: number | null;
  nav_per_share: number | null;
  ev_per_resource_unit: number | null;
  resource_unit: string | null;
  total_attributable_resource: number | null;
  shares_fd: number | null;
  cash_aud: number | null;
  debt_aud: number | null;
  red_flags: string[];
}

export interface RedFlag {
  ticker: string;
  flag_type: string;
  description: string;
}

export interface ReviewData {
  staging: Array<Record<string, unknown>>;
  financials: Array<Record<string, unknown>>;
  resources: Array<Record<string, unknown>>;
  studies: Array<Record<string, unknown>>;
  failed_docs: Array<Record<string, unknown>>;
  red_flags: RedFlag[];
}

export interface CompanyDetail {
  company: Record<string, unknown>;
  financials: Array<Record<string, unknown>>;
  projects: Array<Record<string, unknown>>;
  resources: Resource[];
  studies: Study[];
  documents: Document[];
  drill_results: DrillResult[];
  valuation: Valuation | null;
}

export const api = {
  stats: () => fetchAPI<Stats>("/api/stats"),
  companies: () => fetchAPI<Company[]>("/api/companies"),
  company: (ticker: string) => fetchAPI<CompanyDetail>(`/api/company/${ticker}`),
  documents: (params?: { status?: string; type?: string }) => {
    const search = new URLSearchParams();
    if (params?.status) search.set("status", params.status);
    if (params?.type) search.set("type", params.type);
    const qs = search.toString();
    return fetchAPI<Document[]>(`/api/documents${qs ? `?${qs}` : ""}`);
  },
  valuations: () => fetchAPI<Valuation[]>("/api/valuations"),
  review: () => fetchAPI<ReviewData>("/api/review"),
  resources: (ticker: string) => fetchAPI<Resource[]>(`/api/resources/${ticker}`),
  drill: (ticker: string) => fetchAPI<DrillResult[]>(`/api/drill/${ticker}`),
  parse: (ticker?: string) =>
    fetch(`/api/parse`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker }),
    }).then((r) => r.json()),
  ingest: (tickers: string[], count: number = 10) =>
    fetch(`${API_BASE}/api/ingest`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tickers, count }),
    }).then((r) => r.json()),
};
