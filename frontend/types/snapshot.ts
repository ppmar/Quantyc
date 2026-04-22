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

  exploration?: unknown;
  resource?: unknown;
  holders?: unknown;
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
