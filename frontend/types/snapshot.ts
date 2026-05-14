export type CompanySnapshot = {
  ticker: string;
  name: string;
  exchange: "ASX" | "TSX";
  meta_line: string;
  has_data: boolean;

  cash?: CashSection;
  capital?: CapitalSection;
  cash_history: CashHistoryPoint[];
  activity: ActivityEvent[];
  tabs: TabVisibility;
  projects?: ProjectData[];

  exploration?: unknown;
  resource?: unknown;
  holders?: unknown;
};

export type ResourceRow = {
  category: string;
  tonnes_mt: number | null;
  grade: number | null;
  grade_unit: string | null;
  contained_metal: number | null;
  contained_metal_unit: string | null;
  type: "resource" | "reserve";
  section: string | null;
};

export type StudyData = {
  study_type: string;
  study_date: string | null;
  reporting_currency: string | null;
  discount_rate_pct: number | null;
  post_tax_npv: number | null;
  pre_tax_npv: number | null;
  irr_pct: number | null;
  payback_years: number | null;
  initial_capex: number | null;
  sustaining_capex: number | null;
  opex: number | null;
  aisc_per_unit: number | null;
  aisc_unit: string | null;
  mine_life_years: number | null;
  annual_production: number | null;
  recovery_pct: number | null;
  assumed_fx: number | null;
  price_assumptions: { commodity: string; price: number; unit: string }[] | null;
};

export type RevaluationData = {
  commodity: string;
  price_dfs: number;
  price_spot: number;
  price_unit: string;
  fx_rate: number | null;
  annual_production: number;
  annual_production_unit: string;
  mine_life_years: number;
  discount_rate_pct: number;
  tax_rate_pct: number;
  annuity_factor: number;
  npv_dfs: number;
  npv_spot: number;
  npv_uplift: number;
  npv_uplift_pct: number;
  reporting_currency: string | null;
  method_version: string;
  computed_at: string;
  spot_source: string;
  spot_fetched_at: string;
  warnings: string[];
};

export type ProjectData = {
  name: string;
  stage: string | null;
  state: string | null;
  country: string;
  source: string | null;
  commodities: string[];
  primary_commodity: string | null;
  resources: ResourceRow[];
  resource_date: string | null;
  study: StudyData | null;
  revaluation: RevaluationData | null;
};

export type CashSection = {
  amount_display: string;
  as_of_display: string;
  runway_display: string | null;
  prose: string;
};

export type CapitalSection = {
  shares_display: string;
  shares_label: string;
  prose: string;

  fully_diluted_display?: string;
  overhang_display?: string;
  fully_diluted_prose?: string;
};

export type CashHistoryPoint = {
  quarter: string;
  quarter_end_display: string;
  cash_balance: number;
  burn: number | null;
  burn_display: string | null;
  marker?: "placement" | "options_exercised" | "resource_update" | "drilling_result";
};

export type ActivityEvent = {
  id: string;
  headline: string;
  relative_date: string;
  detail: string;
  source_url?: string;
};

export type TabVisibility = {
  summary: true;
  financials: boolean;
  capital: boolean;
  operations: boolean;
  documents: boolean;
  holders: boolean;
};
