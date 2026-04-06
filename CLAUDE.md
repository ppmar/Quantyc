# ASX Junior Miner Valuation Pipeline — Project Brief for Claude Code

## What this project is

An automated data pipeline that collects, parses, and normalizes ASX junior mining company filings into a structured database, then produces fair-value estimates for each company.

The system answers one question consistently:
**What is this company worth on an attributable, fully diluted, risk-adjusted basis?**

This is NOT a comprehensive mining intelligence platform. It is a lean, valuation-focused input system.

---

## Context you must understand before writing any code

### The Lassonde Curve (stage classification)

Every junior miner sits at a stage on the Lassonde Curve. The stage determines the valuation method:

| Stage | Description | Valuation method |
|---|---|---|
| `concept` | No resource, early drilling | Cash + peer optionality |
| `discovery` | Maiden resource, drill results | EV / attributable resource comps |
| `feasibility` | Scoping / PFS / DFS study | Risked NAV from study NPV |
| `development` | Financing secured, construction | NAV + milestone de-risking |
| `production` | Operating mine | NAV + cash flow multiples |

Stage is inferred from document signals (see classification logic below). It is the single most important field in the database.

### Key mining finance concepts

- **Fully diluted shares**: basic shares + all options + warrants + performance rights + convertibles. Junior miners are almost always misvalued if you use basic shares only.
- **EV (Enterprise Value)**: market cap (fully diluted) + debt + convertibles − cash
- **Quarterly burn**: cash spent per quarter, from Appendix 5B. Determines cash runway.
- **Attributable interest**: the company may own 70% of a project. Only 70% of that project's resource/NPV counts.
- **Royalty/stream burden**: a third party may take 2% NSR or a gold stream. This reduces effective economics.
- **JORC categories**: Measured > Indicated > Inferred. Inferred cannot be used as mine inventory. Always store the category split, never just the total.
- **Capex / Opex**: initial capital to build the mine (capex) vs. ongoing operating cost per tonne or per oz (opex). Both are in study documents.
- **NPV / IRR**: the company's own estimate of project value from a study. Always flag these as company-reported, not verified. The assumed commodity price and FX used in the study must be stored alongside them — a study done at $1,500/oz gold is worth very little when gold is at $2,500/oz.

---

## Architecture

```
ASX announcements feed
        ↓
[1] Collector         — downloads PDFs, records metadata in documents table
        ↓
[2] Classifier        — identifies document type from filename + title keywords
        ↓
[3] Section finder    — finds the relevant pages within a PDF using keyword search
        ↓
[4] Parser            — rule-based for structured docs, LLM for narrative text
        ↓
[5] Staging tables    — raw extracted values with source, method, confidence
        ↓
[6] Normalizer        — resolves units, currencies, gross vs attributable, dilution
        ↓
[7] Core DB           — clean validated values ready for valuation
        ↓
[8] Valuation engine  — produces EV, EV/resource, risked NAV, per-share value
```

---

## Database schema

Use **SQLite** for now (easy to inspect, no server needed). Migrate to Postgres when needed.

### `documents`
```sql
CREATE TABLE documents (
    id              TEXT PRIMARY KEY,   -- sha256 of url
    company_ticker  TEXT NOT NULL,
    doc_type        TEXT,               -- appendix_5b | resource_update | study | capital_raise | annual_report | quarterly_report | other
    announcement_date DATE,
    url             TEXT,
    local_path      TEXT,
    parse_status    TEXT DEFAULT 'pending',  -- pending | done | failed | needs_review
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `companies`
```sql
CREATE TABLE companies (
    ticker          TEXT PRIMARY KEY,
    name            TEXT,
    primary_commodity TEXT,             -- gold | copper | lithium | silver | zinc | etc
    reporting_currency TEXT DEFAULT 'AUD',
    fiscal_year_end TEXT,              -- MM-DD
    updated_at      TIMESTAMP
);
```

### `company_financials`
```sql
CREATE TABLE company_financials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    effective_date  DATE NOT NULL,
    shares_basic    REAL,
    shares_fd       REAL,               -- fully diluted: basic + options + warrants + rights + convertibles
    cash_aud        REAL,
    debt_aud        REAL,
    convertibles_aud REAL,
    quarterly_burn  REAL,               -- cash used in operations + investing, last quarter
    cash_runway_months REAL,            -- derived: cash / quarterly_burn * 3
    last_raise_date DATE,
    last_raise_price REAL,
    last_raise_shares REAL,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,               -- high | medium | low
    needs_review    BOOLEAN DEFAULT 0
);
```

### `projects`
```sql
CREATE TABLE projects (
    id              TEXT PRIMARY KEY,   -- ticker_projectname slug
    ticker          TEXT NOT NULL,
    project_name    TEXT,
    country         TEXT DEFAULT 'Australia',
    state           TEXT,               -- WA | QLD | NSW | SA | NT | VIC | TAS
    stage           TEXT,               -- concept | discovery | feasibility | development | production
    ownership_pct   REAL,
    royalty_type    TEXT,               -- NSR | GRR | stream | none
    royalty_rate    REAL,
    stream_flag     BOOLEAN DEFAULT 0,
    permitting_risk TEXT,               -- low | medium | high | critical
    jurisdiction_risk TEXT,             -- low | medium | high
    is_primary      BOOLEAN DEFAULT 1,  -- is this the company's main asset?
    source_doc_id   TEXT,
    updated_at      TIMESTAMP
);
```

### `project_commodities`
```sql
-- One project can have multiple commodities
CREATE TABLE project_commodities (
    project_id      TEXT NOT NULL,
    commodity       TEXT NOT NULL,      -- gold | silver | copper | lithium | zinc | etc
    is_primary      BOOLEAN DEFAULT 1,
    PRIMARY KEY (project_id, commodity)
);
```

### `resources`
```sql
-- One row per project × commodity × category × effective_date
CREATE TABLE resources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL,
    commodity       TEXT NOT NULL,
    effective_date  DATE,
    estimate_type   TEXT,               -- resource | reserve
    category        TEXT,               -- Measured | Indicated | Inferred | Proven | Probable | Total
    tonnes_mt       REAL,               -- million tonnes
    grade           REAL,
    grade_unit      TEXT,               -- g/t | % | ppm | Li2O%
    contained_metal REAL,
    contained_unit  TEXT,               -- koz | Moz | kt | Mlb | Mt
    attributable_contained REAL,        -- derived: contained_metal * ownership_pct
    cut_off_grade   REAL,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,
    needs_review    BOOLEAN DEFAULT 0
);
```

### `studies`
```sql
-- One row per project × study version
CREATE TABLE studies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL,
    study_stage     TEXT,               -- scoping | pfs | dfs | production
    study_date      DATE,
    mine_life_years REAL,
    annual_production REAL,
    production_unit TEXT,               -- koz/yr | kt/yr | etc
    recovery_pct    REAL,
    initial_capex_musd REAL,
    sustaining_capex_musd REAL,
    opex_per_unit   REAL,
    opex_unit       TEXT,               -- $/oz | $/t
    post_tax_npv_musd REAL,
    irr_pct         REAL,
    assumed_commodity_price REAL,
    assumed_price_unit TEXT,            -- $/oz | $/t | $/lb
    assumed_fx_audusd REAL,
    discount_rate_pct REAL,
    source_doc_id   TEXT,
    extraction_method TEXT,
    confidence      TEXT,
    needs_review    BOOLEAN DEFAULT 0
);
```

### `macro_assumptions`
```sql
CREATE TABLE macro_assumptions (
    date            DATE PRIMARY KEY,
    gold_spot_usd   REAL,
    copper_spot_usd REAL,
    lithium_spot_usd REAL,
    silver_spot_usd REAL,
    aud_usd         REAL,
    base_discount_rate REAL DEFAULT 0.08,
    updated_at      TIMESTAMP
);
```

### `staging_extractions`
```sql
-- Every raw extracted value lands here first, never directly in core tables
CREATE TABLE staging_extractions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id     TEXT NOT NULL,
    field_name      TEXT NOT NULL,
    raw_value       TEXT,
    normalized_value REAL,
    unit            TEXT,
    extraction_method TEXT,             -- rule_based | llm | manual
    confidence      TEXT,               -- high | medium | low
    needs_review    BOOLEAN DEFAULT 0,
    reviewed        BOOLEAN DEFAULT 0,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Document types and how to handle each

### Appendix 5B (quarterly cash flow)
**Structure**: fixed ASX-mandated table format. Rule-based only — no LLM needed.
**Target fields**: `cash_end_quarter`, `operating_cashflow`, `investing_cashflow`
**Parser**: extract tables with pdfplumber, find row containing "cash at end of quarter"
**Confidence**: always `high` if table is found

### Resource/Reserve announcement
**Structure**: JORC table (semi-structured). Try table extraction first, LLM fallback.
**Target fields**: `commodity`, `category`, `tonnes_mt`, `grade`, `grade_unit`, `contained_metal`, `contained_unit`, `effective_date`, `cut_off_grade`
**Parser**: pdfplumber table extraction → if fails → LLM on the 2–3 pages containing JORC keywords
**Confidence**: `high` if table parsed, `medium` if LLM

### Capital raise / issue of securities
**Structure**: semi-structured announcement. Regex first, LLM fallback.
**Target fields**: `new_shares`, `price_per_share`, `total_raised_aud`, `options_attached`, `option_exercise_price`, `option_expiry`
**Parser**: regex for "$X million", "X shares at $Y", LLM for complex terms

### Study announcement (Scoping / PFS / DFS)
**Structure**: narrative + summary table. Always LLM on targeted section.
**Target fields**: `study_stage`, `initial_capex_musd`, `opex_per_unit`, `post_tax_npv_musd`, `irr_pct`, `mine_life_years`, `assumed_commodity_price`, `assumed_fx_audusd`, `recovery_pct`, `annual_production`
**Parser**: find section with keywords ["npv", "irr", "capital cost", "post-tax"], send to LLM with JSON schema

### Annual report
**Structure**: complex, multi-section. Parse only the mineral resources and ore reserves section + balance sheet.
**Target fields**: full resource/reserve table (same as resource announcement), `shares_basic`, `cash`, `debt`
**Parser**: section finder → extract relevant pages → table parser + LLM

---

## LLM extraction rules

**Critical**: never send a full document to the LLM.

1. Use pdfplumber to extract full text page by page
2. Score each page against a keyword list for the target field type
3. Take the top 2–3 pages only (roughly 500–800 tokens of context)
4. Send with a strict JSON schema and the instruction: "Return ONLY valid JSON. Use null for missing fields. Do not invent values."
5. Validate the returned JSON against expected types before writing to staging

### LLM extraction prompt template
```python
EXTRACTION_PROMPT = """You are extracting structured data from an ASX mining company announcement.
Return ONLY a valid JSON object matching this exact schema. Use null for any field not found.
Do not invent or estimate values. Do not add any text outside the JSON.

Schema:
{schema}

Document excerpt:
{chunk}
"""
```

### Section keyword maps
```python
SECTION_KEYWORDS = {
    "resource":  ["mineral resource", "jorc", "measured", "indicated", "inferred", "contained metal", "resource estimate"],
    "cash":      ["cash and cash equivalents", "appendix 5b", "net cash", "cash at end"],
    "capex":     ["capital expenditure", "initial capital", "capex", "capital cost"],
    "npv":       ["net present value", "npv", "irr", "internal rate", "post-tax"],
    "ownership": ["ownership", "earn-in", "joint venture", "royalty", "nsr", "attributable"],
    "shares":    ["shares on issue", "fully diluted", "options on issue", "performance rights"],
}
```

---

## Stage classification logic

```python
STAGE_SIGNALS = {
    "production":  ["commercial production", "first ore", "processing plant commissioning", "quarterly production"],
    "development": ["construction commenced", "financial close", "development decision", "offtake agreement signed"],
    "feasibility": ["definitive feasibility", "dfs", "pre-feasibility", "pfs", "scoping study", "preliminary economic"],
    "discovery":   ["maiden resource", "initial resource", "first resource", "jorc resource"],
    "concept":     [],  # default if nothing else matches
}
```

Classify by scanning the last 6 months of announcement titles for the company. Use the highest stage that appears.

---

## Valuation engine logic

Apply method by stage:

```
concept / discovery:
    EV = market_cap_fd + debt - cash
    EV_per_attributable_oz = EV / sum(attributable_contained where commodity='gold')
    fair_value = peer_median_EV_per_oz * attributable_oz - net_debt
    → output: EV/resource comp, not a NAV

feasibility:
    base_npv = study.post_tax_npv_musd
    price_adjustment = current_commodity_price / study.assumed_commodity_price
    adjusted_npv = base_npv * price_adjustment  # simple linear adjustment
    risk_factor = stage_risk_table[study.study_stage]  # scoping=0.3, pfs=0.5, dfs=0.7
    risked_nav = adjusted_npv * risk_factor * project.ownership_pct
    company_nav = sum(risked_nav across projects) + net_cash - pv_of_ganda
    per_share = company_nav / shares_fd

development / production:
    use NPV directly with smaller risk haircut (0.85–1.0)
    also compute EV/EBITDA if production data available
```

Stage risk table:
```python
STAGE_RISK = {
    "concept":     0.05,
    "discovery":   0.15,
    "feasibility_scoping": 0.25,
    "feasibility_pfs":     0.45,
    "feasibility_dfs":     0.65,
    "development": 0.80,
    "production":  0.95,
}
```

**Red flag triggers** (set `needs_review=True` and add to flag list):
- cash_runway_months < 6
- resource category is >70% Inferred
- study commodity price assumption is >20% below current spot
- study is older than 3 years
- shares_fd > shares_basic * 1.5 (heavy dilution overhang)
- no study exists but company is calling itself a "developer"

---

## ASX data collection

The ASX provides an announcements API:
```
https://www.asx.com.au/asx/1/company/{ticker}/announcements?count=20&market_sensitive=false
```

Returns JSON with announcement metadata including URL to the PDF.

For the PDF itself:
```
https://www.asx.com.au{pdf_path}
```

Rate limit: be polite — add a 1–2 second delay between requests. Do not hammer the API.

**Document type classification from announcement title** (before downloading):
```python
TYPE_KEYWORDS = {
    "appendix_5b":     ["appendix 5b", "quarterly cash flow"],
    "resource_update": ["resource", "reserve", "jorc", "mineral resource"],
    "study":           ["scoping study", "pfs", "dfs", "pre-feasibility", "feasibility study"],
    "capital_raise":   ["placement", "entitlement offer", "rights issue", "issue of securities", "capital raising"],
    "quarterly_report":["quarterly activity", "quarterly report", "operations update"],
    "annual_report":   ["annual report", "annual financial"],
}
```

---

## Project file structure

```
/
├── CLAUDE.md                   ← this file
├── README.md
├── requirements.txt
├── .env                        ← ANTHROPIC_API_KEY
│
├── db/
│   └── schema.sql              ← all CREATE TABLE statements
│
├── pipeline/
│   ├── collector.py            ← downloads PDFs, writes to documents table
│   ├── classifier.py           ← assigns doc_type from title keywords
│   ├── section_finder.py       ← finds relevant pages in a PDF
│   ├── parsers/
│   │   ├── appendix_5b.py      ← rule-based table parser
│   │   ├── resource.py         ← table + LLM fallback
│   │   ├── study.py            ← LLM extraction
│   │   ├── capital_raise.py    ← regex + LLM fallback
│   │   └── llm_extractor.py    ← shared LLM extraction logic
│   ├── normalizer.py           ← units, currencies, gross→attributable
│   └── loader.py               ← staging → core tables
│
├── valuation/
│   ├── engine.py               ← main valuation logic by stage
│   ├── comps.py                ← EV/resource peer comparison
│   └── nav.py                  ← risked NAV calculation
│
├── data/
│   ├── raw/                    ← downloaded PDFs (gitignored)
│   ├── staging.db              ← SQLite database
│   └── macro.csv               ← commodity prices and FX
│
└── review/
    └── exceptions.py           ← query needs_review=True records
```

---

## Tech stack

- **Python 3.11+**
- **pdfplumber** — PDF text and table extraction
- **anthropic** — Claude API for LLM extraction (`claude-sonnet-4-20250514`, max_tokens=800 per call)
- **SQLite** via `sqlite3` (standard library) — no ORM, raw SQL
- **requests** — HTTP for ASX API and PDF download
- **pandas** — normalization and review output
- **python-dotenv** — for ANTHROPIC_API_KEY

No heavy frameworks. Keep it simple.

---

## Pilot companies

Start with these 10 ASX gold juniors to test the pipeline end-to-end before expanding:

Choose a mix of:
- 2 concept/early explorer
- 3 maiden resource / resource growth
- 3 PFS/DFS stage developers
- 2 near-producers

Populate this list in a `pilot_tickers.txt` file once you have researched the current ASX landscape.

---

## Development order

Build in this exact order. Do not skip ahead.

1. `db/schema.sql` — create all tables
2. `pipeline/collector.py` — ASX API → download PDFs → write `documents` table
3. `pipeline/classifier.py` — classify doc_type from title keywords
4. `pipeline/parsers/appendix_5b.py` — rule-based cash extraction (no LLM)
5. `pipeline/parsers/llm_extractor.py` — shared LLM extraction utility
6. `pipeline/section_finder.py` — keyword-based page scoring
7. `pipeline/parsers/resource.py` — JORC table extraction
8. `pipeline/parsers/capital_raise.py` — share/option extraction
9. `pipeline/parsers/study.py` — study economics extraction
10. `pipeline/normalizer.py` — units, attributable, dilution
11. `pipeline/loader.py` — staging → core tables with conflict resolution
12. `valuation/engine.py` — stage-based valuation outputs
13. `review/exceptions.py` — surface all `needs_review=True` records

Test each module on 2–3 pilot companies before moving to the next.

---

## Rules that must never be broken

1. **Every extracted value must have a `source_doc_id`**. No orphan numbers.
2. **Every numeric field must have a unit**. `contained_metal` without `contained_unit` is invalid.
3. **Staging first, always**. Nothing goes directly from parser to core tables.
4. **LLMs never see a full document**. Maximum 3 pages (~800 tokens) of context per call.
5. **Attributable values are always derived from gross × ownership_pct**. Never invent them.
6. **Fully diluted shares = basic + all options + warrants + rights + convertibles**. Never use basic shares for EV.
7. **Study NPVs are always company-reported**. Flag them. Adjust to current commodity prices before using in valuation.
8. **Inferred resources are not reserves**. In the valuation engine, weight Inferred at 0.1–0.2× relative to Indicated/Measured.
