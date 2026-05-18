# SPEC A — OZMIN + MINEDEX Bootstrap Loader

> Audience: Claude Code.
> Goal: Populate the `projects` table with metadata for ASX-listed mining companies, sourced from Geoscience Australia's OZMIN database and Western Australia's MINEDEX database.
> Independent of Track B (parser verification). Files touched do not overlap.

## Context

The `projects` table is currently empty (or near-empty) because it only fills when JORC resource estimate documents are parsed and matched. This SPEC adds an alternative population path: free, structured, public datasets from Australian state and federal geological surveys.

This is **not a parser** and **not an extractor**. It's a one-shot ingestion script that reads pre-structured data and writes to the existing schema.

## Sources

| Source | Authority | Coverage | Format | Endpoint |
|---|---|---|---|---|
| **OZMIN** | Geoscience Australia | National, ~1000 historically-significant deposits | WFS (EarthResourceML 2.0 / ERML-Lite) | `https://services.ga.gov.au/gis/services/ProvinceMineralResourcesMines/MapServer/WFSServer` |
| **MINEDEX** | DMIRS Western Australia | WA only, comprehensive | WFS / bulk download (CSV) | `https://catalogue.data.wa.gov.au/dataset/minedex-dmirs-001` |

Both are public, free, no API key required, Creative Commons licensed.

**Strategic note:** OZMIN coverage skews to historically significant deposits — many ASX juniors won't be in it. MINEDEX is more comprehensive for WA but state-only. Together they cover most of the pilot ticker list; some tickers will still have nothing and rely on the JORC parser path.

## Inviolable constraints

- **Read-only on the source side.** This script only adds rows; it does not delete or modify existing rows in `projects` / `project_commodities`.
- **Idempotent.** Running it twice must not duplicate rows. Match by `(company_id, project_name)` after normalization (case-insensitive, trailing "Project"/"Mine"/"Deposit" stripped).
- **Preserve provenance.** Every row inserted by this loader is tagged so it can be distinguished from parser-sourced rows (see schema change below).
- **No new dependencies.** Use `requests` + `xml.etree.ElementTree` from stdlib for WFS parsing. WFS responses are GML XML; that's tractable without extra libraries.
- **Manual ticker mapping.** OZMIN/MINEDEX records identify projects by name and operator company name, NOT by ASX ticker. The loader must reconcile operator → ticker via a hand-maintained mapping CSV (see `data/ozmin_operator_to_ticker.csv` below). Unmatched operators are logged and skipped — never silently inserted under a guessed ticker.

## Schema change

One column added to `projects`:

```sql
ALTER TABLE projects ADD COLUMN source TEXT;  -- 'ozmin' | 'minedex' | 'jorc_parser' | 'manual'
```

This is the only schema change. No new tables.

Migration file: `db/migrations/0003_projects_source_column.sql`

## Files added

```
data/ozmin_operator_to_ticker.csv      # Hand-maintained operator name → ASX ticker mapping
ingest/ozmin_loader.py                 # OZMIN WFS fetch + project insert
ingest/minedex_loader.py               # MINEDEX CSV fetch + project insert
scripts/load_government_data.py        # CLI orchestrating both loaders
tests/test_ozmin_loader.py             # Synthetic fixtures
tests/test_minedex_loader.py           # Synthetic fixtures
tests/fixtures/ozmin/sample_response.xml
tests/fixtures/minedex/sample_extract.csv
```

## Operator-to-ticker mapping

The hardest part of this SPEC. OZMIN/MINEDEX list operator company names like "De Grey Mining Limited" or "Pilbara Minerals Ltd" — they don't list ASX tickers.

`data/ozmin_operator_to_ticker.csv` format:

```csv
operator_name_normalized,ticker,confidence
de grey mining,DEG,high
pilbara minerals,PLS,high
liontown resources,LTR,high
boss energy,BOE,high
deep yellow,DYL,high
lynas rare earths,LYC,high
west african resources,WAF,high
```

Normalization rules (applied to both source operator names and CSV keys before lookup):
- Lowercase
- Strip suffixes: ` limited`, ` ltd`, ` ltd.`, ` pty`, ` plc`, ` corp`, ` corporation`, ` inc`
- Collapse whitespace to single space
- Trim

Initial CSV must include all entries from `pilot_tickers.txt` that have known operator names. Unmapped operators are logged at WARN level with the exact source string for manual addition.

## OZMIN loader (`ingest/ozmin_loader.py`)

WFS `GetFeature` request pattern:

```
GET https://services.ga.gov.au/gis/services/ProvinceMineralResourcesMines/MapServer/WFSServer
  ?SERVICE=WFS
  &VERSION=2.0.0
  &REQUEST=GetFeature
  &TYPENAME=MineralResources_MineralResourceView
  &OUTPUTFORMAT=application/json
```

Behavior:
1. Fetch the full feature set (bounded — OZMIN is small enough that pagination is unlikely to be needed; if response > 50MB, paginate by `STARTINDEX`/`COUNT` of 1000).
2. Parse JSON (or XML if JSON fails — fall back to ElementTree).
3. For each feature, extract:
   - `deposit_name`
   - `operator_name` (or `holder_name` / `tenement_holder` depending on schema)
   - `commodities` (may be a list)
   - `state`
   - `country` (always "Australia" for OZMIN)
   - `latitude`, `longitude` (NOT stored — schema doesn't have these columns; logged for future use)
   - `mining_status` / `operating_status` → maps to `stage`
4. Normalize operator name and look up in CSV mapping.
5. If no ticker match: log WARN with operator name + project name, skip.
6. If ticker matches but no `companies` row exists: skip with INFO log (the company isn't in our pilot universe).
7. If ticker matches and company exists: upsert to `projects` using `(company_id, normalized_project_name)` as the soft key.
8. For each commodity, upsert into `project_commodities` (mark first commodity as `is_primary=1`).
9. Set `projects.source = 'ozmin'` on insert.
10. On idempotent re-run: existing row found → UPDATE only the columns that are NULL in the existing row. Never overwrite a value that's already populated. This protects parser-sourced data from being clobbered by less-recent OZMIN data.

Stage normalization (OZMIN `operating_status` → `projects.stage`):

| OZMIN value | `projects.stage` |
|---|---|
| `Operating Mine`, `Producer`, `Production` | `production` |
| `Care and Maintenance` | `care_and_maintenance` |
| `Construction`, `Development` | `development` |
| `Feasibility`, `PFS`, `DFS` | `feasibility` |
| `Resource Definition`, `Advanced Exploration` | `advanced_exploration` |
| `Exploration`, `Prospect` | `exploration` |
| (anything else) | NULL |

## MINEDEX loader (`ingest/minedex_loader.py`)

MINEDEX has bulk download (CSV) which is more reliable than WFS. URL discovery:

1. Hit `https://catalogue.data.wa.gov.au/dataset/minedex-dmirs-001` and look for the most recent CSV resource link in the dataset metadata. (Hard-coded URL is brittle; the dataset versions periodically.)
2. Alternative: prompt for manual CSV download to `data/minedex_extract.csv` and have the loader read from there. **Use this path** — it's more robust than scraping for a download URL that changes between releases.

CSV columns expected (verify against current MINEDEX schema before relying):
- `MINEDEX_ID` (unique deposit ID; useful for idempotency)
- `DEPOSIT_NAME`
- `OPERATOR`
- `COMMODITIES` (comma-separated)
- `OPERATING_STATUS`
- `LATITUDE`, `LONGITUDE`
- `STATE` (always WA)

Same flow as OZMIN: normalize operator, ticker lookup, soft-key upsert into `projects`, populate `project_commodities`.

`source = 'minedex'`. On collision with an existing OZMIN-sourced row for the same `(company_id, project_name)`: keep existing fields, update only NULL columns, append `MINEDEX_ID` to a comment column if useful — actually, schema has no comment column. Skip the duplicate with INFO log.

## CLI script (`scripts/load_government_data.py`)

```python
"""
Bootstrap projects table from OZMIN (national) and MINEDEX (WA).

Usage:
    python -m scripts.load_government_data --ozmin --minedex --dry-run
    python -m scripts.load_government_data --ozmin                       # OZMIN only
    python -m scripts.load_government_data --minedex --csv data/minedex_extract.csv
"""
```

Behavior:
- `--dry-run` prints what would be inserted, makes no DB writes.
- Prints a summary at the end:
  ```
  OZMIN:    fetched 1023 features, matched 87 to known tickers, inserted 142 projects, 218 commodities, skipped 936 (unmapped operator)
  MINEDEX:  fetched 4521 records, matched 162 to known tickers, inserted 89 projects (53 already in OZMIN), 134 commodities
  ```
- Non-zero exit code if no matches at all (something is wrong with normalization or the mapping CSV).

## Tests

Synthetic fixtures only — no live network calls in CI.

`tests/fixtures/ozmin/sample_response.json`: a hand-crafted JSON with 5 features covering:
- A known DEG project (Hemi)
- A known PLS project (Pilgangoora)
- An operator without a CSV mapping
- A ticker mapping with no `companies` row
- A duplicate of the first feature (idempotency check)

`tests/test_ozmin_loader.py`:
1. `test_normalize_operator_strips_suffixes` — `"De Grey Mining Limited"` → `"de grey mining"`
2. `test_load_inserts_known_projects` — known operator + existing company → insert
3. `test_load_skips_unmapped_operators` — unmapped operator → no insert, WARN logged
4. `test_load_skips_unknown_tickers` — mapped to ticker but no `companies` row → no insert, INFO logged
5. `test_load_is_idempotent` — run twice, same row count; second run touches no NULL fields
6. `test_load_preserves_existing_non_null_fields` — pre-existing project with `stage='exploration'` (set by parser); OZMIN says `stage='production'`; assert stage stays `exploration`
7. `test_stage_normalization` — covers all OZMIN status → schema stage mappings
8. `test_primary_commodity_marker` — first commodity gets `is_primary=1`, others don't

## Manual verification

After deploying:

```sql
-- How many projects per source?
SELECT source, COUNT(*) FROM projects GROUP BY source;

-- Top tickers by project count
SELECT c.ticker, COUNT(*) AS n
FROM projects p JOIN companies c ON c.company_id = p.company_id
GROUP BY c.ticker ORDER BY n DESC LIMIT 20;

-- Tickers in pilot list with no projects
SELECT ticker FROM companies c
WHERE NOT EXISTS (SELECT 1 FROM projects WHERE company_id = c.company_id);
```

Expected after a successful run with the initial mapping CSV:
- Most pilot tickers should have ≥1 project.
- DEG, PLS, LTR, BOE should each have at least their flagship project.
- Tickers with no government-data presence (e.g., recent IPOs, micro-caps) appear in the "no projects" query — these rely on the JORC parser path.

## Ordering with Track B

This SPEC and Track B (parser verification) are independent. They write to the same `projects` table but via different code paths and with different `source` values. If both run successfully, a row from the parser (`source='jorc_parser'`) and a row from OZMIN (`source='ozmin'`) for the same project should reconcile via the soft-key match — the loaders' "preserve non-NULL fields" rule means parser data wins where it exists.

If you're shipping the Projects tab UI (existing `SPEC_projects_tab.md`), this loader will populate it for many more tickers than the parser path alone.*
