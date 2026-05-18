# SPEC B — JORC Parser Real-World Verification

> Audience: Claude Code.
> Goal: Prove the existing JORC parser works on real ASX MRE PDFs end-to-end. The parser passes 17 synthetic tests but has never been run against a real announcement.
> Independent of Track A (government data bootstrap). Files touched do not overlap.

## Context

`parsers/jorc_resource_estimate.py` was implemented and tested with synthetic ReportLab PDFs. The orchestrator wires `resource_update` documents through `_extract_resource_update()` in `pipeline/orchestrator.py:163`, which writes to `projects`, `project_commodities`, and `resources`.

What we don't know:
- Whether `detect_profile()` correctly accepts real MRE PDFs (synthetic PDFs may not exercise all rejection paths).
- Whether `_find_jorc_tables()` extracts real-world JORC tables, which often have merged cells, multi-line headers, footnotes inline with rows, and varying column orders.
- Whether `_extract_project_name()` regex patterns match real ASX headline language.
- Whether any `resource_update`-classified documents already exist in production with `parse_status='failed'` and what their `parse_error` values are.

This SPEC verifies all of the above on one ticker, then generalizes.

## Inviolable constraints

- No parser modifications until verification reveals a concrete failure.
- No new dependencies.
- All real fixture PDFs go in `tests/fixtures/jorc_resource_estimate/` and are committed to the repo. They're public ASX disclosures — no IP issue.
- Hardcoded test assertions only — no fuzzy "approximately equal" except for `Decimal` tolerance of 0.01 on tonnes/grade, 0.001 on contained metal.

## Files touched

```
scripts/verify_jorc_pipeline.py              # NEW: one-shot end-to-end verifier
scripts/diagnose_resource_updates.py         # NEW: production DB inspection
tests/fixtures/jorc_resource_estimate/       # NEW: directory with real PDFs
tests/test_jorc_resource_estimate_real.py    # NEW: real-fixture test class
```

No edits to `parsers/jorc_resource_estimate.py`, `pipeline/orchestrator.py`, or any schema file unless verification reveals a bug.

## Step 1 — Production diagnosis

Before downloading any fixture, run on Railway:

```bash
python -m scripts.diagnose_resource_updates
```

`scripts/diagnose_resource_updates.py`:

```python
"""
Diagnose the state of resource_update documents and projects/resources tables.

Usage:
    python -m scripts.diagnose_resource_updates
    python -m scripts.diagnose_resource_updates --ticker DEG  # filter
"""
```

Behavior:
- Print counts: total `resource_update` docs, by `parse_status`.
- Print top 10 `parse_error` values with counts.
- Print first 20 `resource_update` documents (ticker, header, parse_status, parse_error, announcement_date) ordered by date desc.
- Print: count of rows in `projects`, `project_commodities`, `resources`.
- Exit 0 always (diagnostic, not validating).

Output goes in the PR description so the next steps are informed by reality.

## Step 2 — Pick the verification ticker

Decision tree based on Step 1 output:

- If ≥1 `resource_update` document exists with `parse_status='parsed'` AND ≥1 row in `resources` → pick that ticker. Confirm the parser already works in production; verification becomes a regression test.
- Else if ≥1 `resource_update` document exists with `parse_status='failed'` → pick the ticker from the most recent failed doc. Verification will reproduce the failure locally.
- Else (no `resource_update` documents at all) → pick **DEG (De Grey Mining)** as the cold-start ticker. DEG has multi-Moz Hemi MRE updates published periodically; well-structured tables.

Fallback order for cold-start: **DEG → LTR (Liontown) → BOE (Boss Energy) → PLS (Pilbara Minerals).**

## Step 3 — Acquire the real fixture

For the chosen ticker:

1. Find the most recent JORC Mineral Resource Estimate announcement on ASX. Search announcement headers for `Mineral Resource` / `Resource Estimate` / `Resource Update`.
2. Download the PDF.
3. Save to `tests/fixtures/jorc_resource_estimate/<TICKER>_mre_<YYYY-MM-DD>.pdf`.
4. Open the PDF and read the headline JORC summary table by hand. Record exact values for the assertions below.

## Step 4 — Author hardcoded test assertions

`tests/test_jorc_resource_estimate_real.py`:

```python
"""Real-fixture tests for the JORC parser. Distinct from the synthetic test suite."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from parsers.jorc_resource_estimate import detect_profile, parse

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "jorc_resource_estimate"


# ─── DEG / Hemi expected values (replace with real values after reading PDF) ───

EXPECTED_DEG_PROJECT_NAME = "Hemi"
EXPECTED_DEG_COMMODITY = "Au"
EXPECTED_DEG_CUTOFF = Decimal("0.5")           # placeholder — read from PDF
EXPECTED_DEG_CUTOFF_UNIT = "g/t"
EXPECTED_DEG_EFFECTIVE_DATE = date(2024, 12, 31)  # placeholder

EXPECTED_DEG_ROWS = [
    # (category, tonnes_mt, grade, grade_unit, contained_metal, contained_metal_unit)
    # placeholders — replace with values read from the PDF
    ("Measured",  Decimal("0"),   Decimal("0"),    "g/t", Decimal("0"),    "Moz"),
    ("Indicated", Decimal("0"),   Decimal("0"),    "g/t", Decimal("0"),    "Moz"),
    ("Inferred",  Decimal("0"),   Decimal("0"),    "g/t", Decimal("0"),    "Moz"),
    ("Total",     Decimal("0"),   Decimal("0"),    "g/t", Decimal("0"),    "Moz"),
]


@pytest.fixture(scope="module")
def deg_pdf_bytes():
    pdf_path = FIXTURE_DIR / "DEG_mre_2024-12-31.pdf"  # filename matches saved fixture
    if not pdf_path.exists():
        pytest.skip(f"DEG fixture not found: {pdf_path}")
    return pdf_path.read_bytes()


@pytest.fixture(scope="module")
def deg_result(deg_pdf_bytes):
    return parse(
        pdf_bytes=deg_pdf_bytes,
        ticker="DEG",
        doc_id="real_deg_001",
        announcement_date=date(2025, 1, 15),  # the announcement date, not the effective date
    )


class TestDEGRealFixture:
    def test_detect_profile_accepts(self, deg_pdf_bytes):
        assert detect_profile(deg_pdf_bytes) is True

    def test_project_name(self, deg_result):
        assert deg_result.project_name == EXPECTED_DEG_PROJECT_NAME

    def test_commodity(self, deg_result):
        assert deg_result.commodity == EXPECTED_DEG_COMMODITY

    def test_cutoff(self, deg_result):
        assert deg_result.cutoff_grade == EXPECTED_DEG_CUTOFF
        assert deg_result.cutoff_grade_unit == EXPECTED_DEG_CUTOFF_UNIT

    def test_effective_date(self, deg_result):
        assert deg_result.snapshot_date == EXPECTED_DEG_EFFECTIVE_DATE

    def test_categories_present(self, deg_result):
        categories = {r.category for r in deg_result.rows}
        # at minimum, expect Inferred (every junior MRE has at least Inferred)
        assert "Inferred" in categories

    def test_rows_match_expected(self, deg_result):
        TONNES_TOL = Decimal("0.1")
        GRADE_TOL = Decimal("0.01")
        CONTAINED_TOL = Decimal("0.01")

        actual_by_cat = {r.category: r for r in deg_result.rows}

        for expected in EXPECTED_DEG_ROWS:
            cat, exp_tonnes, exp_grade, exp_grade_unit, exp_contained, exp_contained_unit = expected
            assert cat in actual_by_cat, f"Missing category: {cat}"
            actual = actual_by_cat[cat]
            assert abs(actual.tonnes_mt - exp_tonnes) < TONNES_TOL, \
                f"{cat}: tonnes {actual.tonnes_mt} vs expected {exp_tonnes}"
            assert abs(actual.grade - exp_grade) < GRADE_TOL, \
                f"{cat}: grade {actual.grade} vs expected {exp_grade}"
            assert actual.grade_unit == exp_grade_unit
            assert abs(actual.contained_metal - exp_contained) < CONTAINED_TOL, \
                f"{cat}: contained {actual.contained_metal} vs expected {exp_contained}"
            assert actual.contained_metal_unit == exp_contained_unit

    def test_no_warnings(self, deg_result):
        # Any warning on a real MRE indicates silent extraction degradation
        assert deg_result.extraction_warnings == [], \
            f"Unexpected warnings: {deg_result.extraction_warnings}"
```

Run:

```bash
pytest tests/test_jorc_resource_estimate_real.py -v
```

If tests fail: do not modify the parser yet. First, log the actual values returned by `parse()` and compare to expected. The failure mode (project name regex miss vs table parse miss vs cutoff regex miss) determines the fix scope.

## Step 5 — End-to-end pipeline verification

`scripts/verify_jorc_pipeline.py`:

```python
"""
End-to-end verification of the JORC pipeline on a single fixture.

Inserts a synthetic documents row, runs the production codepath
(_extract_resource_update from pipeline.orchestrator), and prints
the resulting projects / project_commodities / resources rows.

Usage:
    python -m scripts.verify_jorc_pipeline DEG tests/fixtures/jorc_resource_estimate/DEG_mre_2024-12-31.pdf
"""
```

Behavior:
1. Validate args; exit 1 if PDF doesn't exist.
2. Compute deterministic synthetic doc URL: `f"verify://jorc/{ticker}/{filename}"`.
3. Compute SHA256 over the PDF bytes for the `documents.sha256` column.
4. Upsert into `companies` (ticker only) if not present.
5. Upsert a `documents` row with `doc_type='resource_update'`, `parse_status='classified'`, real announcement date (parsed from filename).
6. Read the PDF bytes, call `_extract_resource_update(doc_id, pdf_bytes, ticker, announcement_date, stats)`.
7. Query and print:
   - `parse_status` and `parse_error` of the document
   - All rows in `projects` for that ticker
   - All rows in `project_commodities` for those projects
   - All rows in `resources` for those projects
8. Exit 0 if `parse_status='parsed'` and ≥1 row in `resources`. Exit 1 otherwise with the failure reason.

Re-running the script on the same PDF must be idempotent (the SHA256 unique constraint protects against duplicate documents).

## Step 6 — Run on Railway

After Step 4 tests pass locally:

1. Deploy the new files (the script is a CLI; no production code path changes).
2. Run on Railway:
   ```bash
   python -m scripts.verify_jorc_pipeline DEG tests/fixtures/jorc_resource_estimate/DEG_mre_2024-12-31.pdf
   ```
3. Verify with SQL:
   ```sql
   SELECT * FROM projects WHERE company_id = (SELECT company_id FROM companies WHERE ticker='DEG');
   SELECT * FROM resources WHERE project_id IN (
     SELECT project_id FROM projects WHERE company_id = (SELECT company_id FROM companies WHERE ticker='DEG')
   );
   ```

Expected: 1 project ("Hemi"), 1 commodity ("Au", primary), ≥3 resource rows (Measured/Indicated/Inferred or Inferred + Total).

## Step 7 — Generalize (only after Step 6 succeeds)

Add fixtures and corresponding test classes for:

1. **LTR (Liontown — Kathleen Valley Li2O)** — different commodity, different grade unit (`%`).
2. **BOE (Boss Energy — Honeymoon U3O8)** — different commodity, ppm grade unit.
3. **One reserve-only fixture** if available (e.g., a flagship gold producer's annual reserves statement) — verifies the reserve detection path.
4. **One complex fixture** with multi-domain table (separate Open Pit vs Underground rows) — tests robustness; if it fails, document the failure and add a known-limitation entry rather than over-engineering the parser.

Each new fixture follows the same pattern: copy the test class, replace expected values, run.

## Failure debugging guide

If `detect_profile()` returns False:
- Add `print(text[:2000])` at the top of `parse()` in a temporary debug branch to see what page 1 actually contains.
- Check whether `_DISQUALIFIER_PATTERNS` are triggering on body text (e.g., a footer mentions "Quarterly Activities Report").
- Loosen the disqualifier check to first 1000 chars only.

If `_extract_project_name()` returns None:
- Print the first 2000 chars of page 1 text.
- Add a new pattern to `_extract_project_name`. Real announcements often have a layout like:
  ```
  HEMI GOLD PROJECT
  MINERAL RESOURCE UPDATE
  ```
  Multi-line patterns may not match the existing single-line regexes.

If `_find_jorc_tables()` returns empty:
- Use `pdfplumber.open(pdf_path).pages[i].extract_tables()` interactively in a notebook to see what tables are detected.
- Real tables often have merged header cells that pdfplumber renders as `None`. The header detection logic may need to look across the first 2 rows, not just the first row.

If categories are present but values are wrong:
- Most common cause: tonnes are in `kt` not `Mt` and `_normalize_tonnes` isn't catching the unit. Print `headers` to see what the raw column header text was.
- Second most common: the table has a "Sub-Total" or "Subtotal" row mixed in with category rows. Existing parser doesn't handle that. Add to JORC_ORDER as `Subtotal` mapped to a special category, or filter it out.

## Out of scope

- Parser refactoring beyond minimum fixes for the verification fixture.
- Multi-quarter resource history. The parser writes one estimate per (project, effective_date); whether that history accumulates is a question for the orchestrator dispatch, not this SPEC.
- Polymetallic deposits with co-equal commodities (e.g., copper-gold porphyries). Existing parser picks a primary commodity with a warning; that's acceptable.
- Reserve parsing as primary path. Reserves emit a warning and are skipped in resource-only mode; that's the documented behavior.
