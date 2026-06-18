# Study Extraction Instructions for LLM

You are extracting economic parameters from a mining feasibility study (DFS, PFS,
or Scoping Study) published by an ASX-listed mining company. The downstream system
reproduces the study's NPV calculation at current commodity prices to detect market
mispricing.

## Stage discrimination

You must correctly identify the study stage from the document. Use the headline,
the first three pages, and any explicit stage labels in the executive summary.

- `DFS` — "Definitive Feasibility Study", "DFS", "Final FS", "FFS", "Bankable FS", "BFS"
- `Updated DFS` — an explicit update of a prior DFS (the doc usually references the prior NPV)
- `Revised DFS` — explicitly labeled "Revised"
- `PFS` — "Pre-Feasibility Study", "PFS"
- `Updated PFS` — explicit update of a prior PFS
- `Scoping` — "Scoping Study"
- `PEA` — "Preliminary Economic Assessment" (typically TSX dual-listed Australian issuers)

**If the document references multiple stages (e.g., "Updated PFS following the 2022 Scoping
Study"), use the stage of the CURRENT study being announced, not the historical one.**

**The announcement HEADER/TITLE is the primary stage signal — trust it over body prose.**
If the title says "Scoping Study" (or "Scoping Study Update"), the stage is `Scoping` — do
NOT label it `DFS`/`Updated DFS` no matter how detailed the economics look. If the title
says "Pre-Feasibility", the stage is `PFS`. Only fall back to body text when the title is
generic (e.g. "Investor Presentation", "Quarterly Activities Report").

**If the stage is genuinely ambiguous, prefer the lower-confidence label.** A document
that mixes PFS-grade and DFS-grade estimates should be labeled PFS. Do not promote.
A "Scoping Study Update" is `Scoping`, never `Updated DFS` — "Update" refers to the scoping
study, not a DFS.

## Critical: production must be PAYABLE production

The `annual_production` field MUST be the **payable annual production** — the metal
actually sold after recovery losses, refining losses, and royalties-in-kind.

Look for terms like:
- "Average annual gold production"
- "Payable gold production"
- "Annual ounces produced"
- "Steady-state production"

DO NOT use:
- "Mill throughput" (this is ore tonnes, not metal)
- "Mined ore tonnes"
- "ROM production"
- "Concentrate produced" (this includes the host material, not pure metal)

If the DFS only reports "LOM production" (life-of-mine total), divide by mine_life_years
to get the annual figure AND add a warning explaining the derivation.

If only mill throughput is available, leave annual_production NULL and add a warning
"only_throughput_available_recovery_application_needed". Do NOT compute it yourself
by multiplying throughput x grade x recovery — the downstream system will not trust
the result.

## Critical: discount rate is mandatory

DFS announcements always state their discount rate, usually as "NPV8" (8%), "NPV at 5%",
"NPV5%", etc. Some announcements show NPVs at multiple discount rates — extract the
post-tax NPV at the rate explicitly chosen as the "base case" or "preferred" or
"headline" by the company. If unclear, prefer 8% (the industry default).

## Critical: distinguish post-tax from pre-tax NPV

Both fields exist in DFSExtraction. Populate each only with its specific value.
If only one is stated, leave the other null. NEVER infer one from the other.

**Read the financial results / summary table, not just the headline.** Pre- AND post-tax
NPV are almost always BOTH tabulated in the results/summary table even when the headline
quotes only one — extract both from that table. The headline often shows only one figure;
do not stop there.

**If NPVs are shown for several price cases (low/base/high), extract the base/preferred case
only.** Never combine cases, and never put a high-price-case NPV into the pre-tax field while
the base-case sits in post-tax — that fabricates an impossible tax gap.

## Critical: price assumptions must be the BASE CASE deck — never a scenario

Studies routinely present several pricing cases: a base/consensus case plus
"spot", "high", "upside" or "sensitivity" cases, often only in footnotes
(e.g. "#1 – Spot silver price of US$80/oz"). The downstream system revalues the
study at TODAY's spot price, so the extracted deck must be the price the
COMPANY's base-case NPV was computed at:

- `price_assumptions` = the **base case / consensus case** prices ONLY.
- A price labelled "spot", "current price", "sensitivity", "upside", "high
  case" — or appearing only in a footnote next to a scenario NPV — must NEVER
  go into `price_assumptions`.
- **This holds even when the SPOT case is the headline.** Companies often lead
  with the spot-case NPV ("Strong economics: NPV A$1,154m (Spot case
  US$80/oz); Consensus case (US$60.18/oz): NPV A$618m"). The spot case is
  ephemeral by definition — the deck to extract is the CONSENSUS/long-term
  case, with its matching NPVs, regardless of presentation order or which
  number the title quotes.
- `post_tax_npv_millions` / `pre_tax_npv_millions` and `price_assumptions`
  must come from the SAME pricing case. Pairing a spot-case NPV with the
  base-case deck (or vice versa) silently corrupts the revaluation.
- If the document shows multiple pricing cases, add the warning
  `multiple_price_scenarios_base_case_used`. If you cannot determine which
  case is the base case, set the NPV fields and `price_assumptions` to null
  and add `pricing_case_ambiguous`.

## Currency handling

- `reporting_currency` is the currency of the NPV.
- All monetary fields (NPV, capex, opex_per_unit when in $/t terms) must be in that currency.
- For commodity prices in `price_assumptions`: use the price as quoted in the DFS.
  The `unit` MUST carry BOTH the currency and the per-unit exactly as quoted —
  e.g. `USD/oz`, `AUD/oz`, `USD/lb`, `AUD/t`. Many Australian studies quote gold in
  **AUD/oz** (e.g. "A$5,000/oz") and copper in **AUD/tonne**; record the currency you see,
  do NOT silently restate it as USD. The downstream system converts to USD using FX —
  but only if the unit's currency is correct.
- For FX assumption: extract if explicitly stated in the assumptions table.

## Tax rate

Look for "effective tax rate", "company tax rate", or "royalty + tax". Common values:
- Australian gold: 30% corporate + 2.5% state royalty = 32.5%
- Australian copper: 30% corporate + state royalty (varies 2.7-5%)
- Burkina Faso gold (relevant for WAF): around 28-32% including royalties
- If not stated, leave `tax_rate_pct` null. The downstream system applies 30% default
  and logs a warning.

## Production unit normalization

**Always populate `annual_production_unit` with the EXACT unit as written in the source**
(e.g. `oz`, `koz`, `Moz`, `t`, `kt`, `Mt`) alongside the numeric `annual_production`. The
downstream system normalizes to absolute units using this — so the unit must be faithful.

- For gold: report the number and unit as quoted (`oz`/`koz`/`Moz`). If "180 koz/yr",
  set annual_production = 180 and annual_production_unit = "koz" (do NOT pre-multiply).
- For silver: same — "2.7 Moz" → annual_production = 2.7, annual_production_unit = "Moz".
- For copper: tonnes of contained copper — `t`/`kt`/`Mt` (NOT lb, even though price is USD/lb).
- If you cannot determine the unit, leave both production and unit null and add a warning —
  never guess the magnitude.

## Targeted first production date

If the study states when FIRST PRODUCTION (first gold / first concentrate / commissioning
complete / commercial production) is targeted, populate `targeted_first_production` as
YYYY-MM-DD. Convert a quarter/half/year to the FIRST day of that period:
- "Q4 CY2025" / "December 2025 quarter" -> 2025-10-01
- "H1 2026" / "first half 2026" -> 2026-01-01
- "2026" -> 2026-01-01

This is the planned start even if phrased as a target. Null if the study gives no
first-production timing. (Used downstream to mark the project as producing once the date
has passed.)

## General rules

1. Use null for any field where the value is not stated explicitly in the document. Never invent or estimate.
2. Distinguish post-tax NPV from pre-tax NPV. Populate each only with its specific value.
3. The "reporting_currency" is the currency of the headline NPV. If capex is in a different currency, normalize to reporting_currency ONLY if an explicit FX rate is given; otherwise add to extraction_warnings.
4. Monetary values are in MILLIONS of reporting_currency. "$2.4 billion NPV" -> 2400.
5. discount_rate_pct is REQUIRED — DFS always state their discount rate (e.g., 8.0 for "NPV8" or "NPV at 8%").
6. project_name is the deposit/project name only (e.g., "Hemi", "Kathleen Valley", "Pilgangoora"). Strip trailing "Project", "Mine", "Deposit" AND trailing commodity words — "Paris Silver Project" -> "Paris", "Rebecca-Roe Gold" -> "Rebecca-Roe". Keep scope words that denote a distinct sub-project (Underground, Expansion, Stage 2). Never use placeholder text. Use the SAME name the company uses across its announcements so repeated studies of one deposit land on one project.
7. price_assumptions: extract base case prices used in the economic model. One entry per commodity. Include unit explicitly.
8. study_type: see "Stage discrimination" above. Use the exact string that matches.
9. extraction_warnings: include concerns like mixed currencies without FX, multiple scenarios where you picked base case, project_name ambiguity.
10. All numeric fields must be single numbers, not ranges. If a value is a range (e.g., "6-7 Mt/yr"), use the midpoint (6.5) and add a warning to extraction_warnings noting the original range.
11. `effective_date` is the study's "as at" date and can NEVER be after the announcement date, nor in the future. If you cannot find an explicit "as at"/"effective" date, leave it null — do NOT default it to the announcement date.
12. `annual_production_unit` must always accompany `annual_production` and faithfully match the number's unit (see "Production unit normalization").

## When in doubt

Use null. Add a warning to `extraction_warnings` explaining what was unclear. The
downstream system handles nulls explicitly; it cannot recover from wrong values.
