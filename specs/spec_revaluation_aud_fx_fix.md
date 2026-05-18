# SPEC: Revaluation AUD branch + FX convention fix

**Status:** ready for Claude Code
**Scope:** `revaluation/math.py`, `revaluation/pipeline.py`, `revaluation/prices.py`, three test modules
**Out of scope:** any change to `parsers/dfs_study*` (the extraction convention is the source of truth)
**Method version bump:** YES — `first_order_v1` → `first_order_v2`

---

## 1. Problem

Three revaluation tests currently fail on `main`:

```
FAILED tests/test_revaluation_math.py::test_revalue_gold_aud_reporting
FAILED tests/test_revaluation_prices.py::test_fetch_yahoo_quote_gold
FAILED tests/test_revaluation_prices.py::test_fetch_yahoo_quote_no_price_raises
```

Root causes:

**(a) AUD math branch is wrong.** `revaluation/math.py:130-138` treats `price_dfs_usd` as already-in-AUD when `reporting_currency == "AUD"`. This contradicts `parsers/dfs_study_prompt.md:47-49` which states DFS prices are stored in USD **regardless of reporting currency**. Result on the Hemi-like test scenario: code outputs `AU$1,333 M` instead of the correct `AU$3,398 M` — a ~60% understatement of NPV.

**(b) FX convention is ambiguous and inconsistent.**
- `revaluation/prices.py:18-22` annotates `AUDUSD=X` with unit `"AUD/USD"` — ambiguous notation.
- `revaluation/math.py:133` comment claims `fx_rate` is "AUDUSD (0.7225)" — Yahoo convention (USD per AUD).
- `tests/test_revaluation_math.py:55` and `tests/test_revaluation_pipeline.py:172` mock `fx_rate=1.55` — the inverse (AUD per USD).
- `tests/test_revaluation_pipeline.py:150-167` contains a comment block where the author noticed the inconsistency but punted.

**(c) Yahoo fetcher mock shape is stale.** `prices.py:33-55` migrated to the v8 `/chart/` endpoint (response shape `chart.result[0].meta.regularMarketPrice`), but `tests/test_revaluation_prices.py:38-72` still mocks the old v7 `quoteResponse` shape.

---

## 2. Invariants (inviolable)

These rules are the contract. Do not change them; change code and tests to comply.

**I1.** DFS prices stored in `studies.assumed_price_deck` are **always USD per canonical unit** (USD/oz for Au, USD/lb for Cu). Source: `parsers/dfs_study_prompt.md:47-49`. No exceptions.

**I2.** `RevaluationInput.price_dfs_usd` is **always USD**, regardless of `reporting_currency`. The field name reflects this. Do not rename it in this PR — too many call sites.

**I3.** `RevaluationInput.price_spot_usd` is always USD. Source: Yahoo `GC=F` and `HG=F` symbols.

**I4.** `RevaluationInput.fx_rate` follows the **Yahoo `AUDUSD=X` convention**: it is USD per 1 AUD (typically ~0.65 in 2024-2026). To convert any USD amount to AUD: divide by `fx_rate`. To convert any AUD amount to USD: multiply by `fx_rate`. This is the only allowed FX convention in the codebase.

**I5.** `revaluations.npv_dfs`, `revaluations.npv_spot`, `revaluations.npv_uplift` are stored in the **study's reporting currency** in millions. They are directly comparable to `studies.post_tax_npv`.

**I6.** `revaluations.method_version` bumps to `first_order_v2` for any row written by the fixed code. Old `first_order_v1` rows for AUD-reporting studies are known to be incorrect — see PR3 backfill section.

**I7.** The math layer is pure: no DB, no network, no `datetime.now`, no I/O. All inputs explicit. Already true; do not regress.

---

## 3. PR sequence

Four PRs, each independently verifiable. Do not collapse them.

### PR1 — Standardize FX convention in `prices.py`

**File:** `revaluation/prices.py`

**Changes:**

1. Update `SYMBOL_MAP` (line 18-22) entry for `AUDUSD=X` to use unambiguous unit string:
   ```python
   "AUDUSD=X": ("AUDUSD", "USD_per_AUD", Decimal("1")),
   ```

2. Add a module-level docstring section documenting the FX convention (insert after the existing module docstring, before imports):
   ```python
   # FX convention (invariant I4):
   #   The "AUDUSD" commodity is fetched via the Yahoo AUDUSD=X symbol.
   #   The returned scalar is "USD per 1 AUD" (typically ~0.65).
   #   To convert amount_usd -> amount_aud:  amount_usd / fx_rate
   #   To convert amount_aud -> amount_usd:  amount_aud * fx_rate
   #   This convention is enforced in revaluation/math.py.
   ```

3. No behavioral change to `fetch_yahoo_quote` or `get_or_fetch_price`.

**Test additions:** none (covered in PR4).

**Acceptance:** `python -c "from revaluation.prices import SYMBOL_MAP; assert SYMBOL_MAP['AUDUSD=X'][1] == 'USD_per_AUD'"` passes.

---

### PR2 — Fix AUD branch in `math.py`

**File:** `revaluation/math.py`

**Changes:**

1. Bump `METHOD_VERSION` (line 11):
   ```python
   METHOD_VERSION = "first_order_v2"
   ```

2. Replace the entire `revalue()` function body's currency-handling block (lines 124-149) with the corrected logic:

   ```python
   # Annuity factor
   a = annuity_factor(inp.discount_rate_pct, inp.mine_life_years)

   # Both prices are USD per invariant I2/I3. Compute uplift in USD.
   delta_price_usd = inp.price_spot_usd - inp.price_dfs_usd
   delta_revenue_annual_usd = normalized_production * delta_price_usd
   delta_npv_usd = delta_revenue_annual_usd * a * (Decimal("1") - tax_rate / Decimal("100"))
   delta_npv_usd_millions = delta_npv_usd / Decimal("1000000")

   # Convert to reporting currency per invariant I4.
   if inp.reporting_currency == "USD":
       delta_npv_reporting_currency = delta_npv_usd_millions
   elif inp.reporting_currency == "AUD":
       if inp.fx_rate is None:
           raise RevaluationError("fx_rate_required_for_aud_reporting")
       if inp.fx_rate <= 0:
           raise RevaluationError(f"fx_rate_must_be_positive:{inp.fx_rate}")
       # fx_rate = USD per AUD (~0.65). amount_aud = amount_usd / fx_rate.
       delta_npv_reporting_currency = delta_npv_usd_millions / inp.fx_rate
   else:
       raise RevaluationError(f"unsupported_reporting_currency:{inp.reporting_currency}")

   npv_spot = inp.npv_dfs + delta_npv_reporting_currency
   npv_uplift = npv_spot - inp.npv_dfs
   npv_uplift_pct = (npv_uplift / inp.npv_dfs) if inp.npv_dfs != 0 else Decimal("0")
   ```

3. Remove `fx_divisor` (dead code, was never read after assignment).

4. Update the docstring on `RevaluationInput.fx_rate` (line 29-30) to:
   ```python
   fx_rate: Optional[Decimal]       # USD per 1 AUD, e.g. 0.6452 (Yahoo AUDUSD=X convention).
                                    # Required when reporting_currency == "AUD". Used as:
                                    # amount_aud = amount_usd / fx_rate
   ```

5. Update the docstring on `RevaluationResult.delta_revenue_annual_usd` to clarify it is genuinely USD (not reporting currency).

**Acceptance:** see PR3 tests.

---

### PR3 — Repair and extend math/pipeline tests

**File:** `tests/test_revaluation_math.py`

**Changes:**

1. Replace `test_revalue_gold_aud_reporting` (lines 42-68) with the corrected expectation. Inputs unchanged except `fx_rate` switches to Yahoo convention:

   ```python
   def test_revalue_gold_aud_reporting():
       """Hemi-like scenario: DFS price in USD, NPV reported in AUD,
       FX is Yahoo AUDUSD=X convention (USD per AUD)."""
       inp = RevaluationInput(
           commodity="Au",
           price_dfs_usd=Decimal("1900"),       # USD per invariant I2
           price_spot_usd=Decimal("3500"),      # USD per invariant I3
           annual_production=Decimal("180000"),
           annual_production_unit="oz",
           mine_life_years=Decimal("10"),
           discount_rate_pct=Decimal("5.0"),
           tax_rate_pct=Decimal("30.0"),
           npv_dfs=Decimal("985"),              # AUD M
           reporting_currency="AUD",
           fx_rate=Decimal("0.6452"),           # USD per AUD (Yahoo convention)
       )
       result = revalue(inp)
       # ΔPrice_USD = 1600 USD/oz
       # ΔRev_USD  = 180,000 * 1600 = 288,000,000 USD/yr
       # A(5%,10)  = 7.7217
       # ΔNPV_USD  = 288e6 * 7.7217 * 0.70 / 1e6 = 1556.70 USD M
       # ΔNPV_AUD  = 1556.70 / 0.6452              = 2412.74 AUD M
       # NPV_spot  = 985 + 2412.74                 = 3397.74 AUD M
       assert abs(result.npv_spot - Decimal("3397.74")) < Decimal("0.10")
       assert abs(result.npv_uplift - Decimal("2412.74")) < Decimal("0.10")
       assert abs(result.delta_revenue_annual_usd - Decimal("288000000")) < Decimal("1")
       assert result.method_version == "first_order_v2"
       assert result.warnings == []
   ```

2. Add a positive USD-reporting test using the same inputs (to anchor the USD path with magnitude assertions, not just signs):

   ```python
   def test_revalue_gold_usd_reporting_magnitude():
       """Same scenario but reporting in USD: no FX conversion."""
       inp = RevaluationInput(
           commodity="Au",
           price_dfs_usd=Decimal("1900"),
           price_spot_usd=Decimal("3500"),
           annual_production=Decimal("180000"),
           annual_production_unit="oz",
           mine_life_years=Decimal("10"),
           discount_rate_pct=Decimal("5.0"),
           tax_rate_pct=Decimal("30.0"),
           npv_dfs=Decimal("985"),
           reporting_currency="USD",
           fx_rate=None,
       )
       result = revalue(inp)
       # NPV_spot = 985 + 1556.70 = 2541.70 USD M
       assert abs(result.npv_spot - Decimal("2541.70")) < Decimal("0.10")
       assert abs(result.npv_uplift - Decimal("1556.70")) < Decimal("0.10")
   ```

3. Add a Sanbrado-grade USD test using values back-solved from the existing UI fixture (the image showed the displayed inputs do not exactly reproduce the displayed outputs because production is rounded for display; we test the math, not the rounding):

   ```python
   def test_revalue_sanbrado_au_usd():
       """Sanbrado Gold (West African Resources) — USD-reporting DFS.
       Production back-solved from displayed uplift = 4063 M at 3384 USD/oz uplift,
       11yr life, 5% rate, 32% tax: production ≈ 212,580 oz/yr.
       """
       inp = RevaluationInput(
           commodity="Au",
           price_dfs_usd=Decimal("1300"),
           price_spot_usd=Decimal("4684"),
           annual_production=Decimal("212580"),
           annual_production_unit="oz",
           mine_life_years=Decimal("11"),
           discount_rate_pct=Decimal("5.0"),
           tax_rate_pct=Decimal("32.0"),
           npv_dfs=Decimal("405"),
           reporting_currency="USD",
           fx_rate=None,
       )
       result = revalue(inp)
       # Hand-computed: ΔNPV ≈ 4063 M, NPV_spot ≈ 4468 M
       assert abs(result.npv_spot - Decimal("4468")) < Decimal("5")
       assert abs(result.npv_uplift - Decimal("4063")) < Decimal("5")
   ```

4. Add a negative-FX validation test:

   ```python
   def test_aud_reporting_zero_fx_raises():
       inp = RevaluationInput(
           commodity="Au", price_dfs_usd=Decimal("1900"),
           price_spot_usd=Decimal("3500"),
           annual_production=Decimal("180000"), annual_production_unit="oz",
           mine_life_years=Decimal("10"), discount_rate_pct=Decimal("5"),
           tax_rate_pct=Decimal("30"), npv_dfs=Decimal("985"),
           reporting_currency="AUD", fx_rate=Decimal("0"),
       )
       with pytest.raises(RevaluationError, match="fx_rate_must_be_positive"):
           revalue(inp)
   ```

**File:** `tests/test_revaluation_pipeline.py`

5. Replace the comment block at lines 150-167 (the "I am confused" block) with a single line:
   ```python
   # Yahoo AUDUSD=X returns USD per AUD (~0.6452). See invariant I4 in SPEC_revaluation_aud_fx_fix.md.
   ```

6. In `test_revalue_study_end_to_end_au`, change the Yahoo mock for AUDUSD from `1.55` to `Decimal("0.6452")` (line 172).

7. Add magnitude assertions after the existing directional ones (lines 188-190 currently only check signs):
   ```python
   # Hand-checked: with spot=3520, fx=0.6452, NPV_DFS=985 AUD M
   # ΔNPV_USD = 180,000 * (3520-1900) * 7.7217 * 0.70 / 1e6 = 1576.16 USD M
   # ΔNPV_AUD = 1576.16 / 0.6452                              = 2443.00 AUD M
   # NPV_spot = 985 + 2443.00                                 = 3428.00 AUD M
   assert abs(row["npv_spot"] - 3428.00) < 1.0
   assert abs(row["npv_uplift"] - 2443.00) < 1.0
   assert row["method_version"] == "first_order_v2"
   ```

**Acceptance:**
- `pytest tests/test_revaluation_math.py tests/test_revaluation_pipeline.py -v` → all green.
- 22 tests previously passing still pass; the 1 AUD failure flips to green; 4 new tests pass.

---

### PR4 — Fix stale Yahoo mock shape in `test_revaluation_prices.py`

**File:** `tests/test_revaluation_prices.py`

The production code in `prices.py:33-55` uses the v8 `/chart/` endpoint with shape `chart.result[0].meta.regularMarketPrice`. Two tests still mock v7 `quoteResponse` shape and currently fail.

**Changes:**

1. `test_fetch_yahoo_quote_gold` (lines 38-48): update mock JSON to v8 shape:
   ```python
   mock_resp.json.return_value = {
       "chart": {
           "result": [{"meta": {"regularMarketPrice": 3520.5, "symbol": "GC=F"}}]
       }
   }
   ```

2. `test_fetch_yahoo_quote_empty_result_raises` (lines 51-59): update mock to v8 empty shape:
   ```python
   mock_resp.json.return_value = {"chart": {"result": []}}
   ```

3. `test_fetch_yahoo_quote_no_price_raises` (lines 62-72): update mock to v8 missing-price shape:
   ```python
   mock_resp.json.return_value = {
       "chart": {"result": [{"meta": {"symbol": "GC=F"}}]}
   }
   ```

**Acceptance:** `pytest tests/test_revaluation_prices.py -v` → all green.

---

## 4. Migration / data hygiene

After PR1-4 land, existing rows in `revaluations` with `method_version = 'first_order_v1'` AND `study.reporting_currency = 'AUD'` are stale (computed with the buggy AUD branch). Two options:

- **Option A (recommended):** add a one-shot script `scripts/recompute_v1_aud_revaluations.py` following the existing `scripts/` conventions. For each affected row, call `revalue_study(conn, study_id)` to write a new row with `first_order_v2`. Do not delete v1 rows — keep for audit.
- **Option B (lightweight):** add a query helper that filters by latest `method_version` per study; defer recomputation until needed.

Pick A if `SELECT COUNT(*) FROM revaluations WHERE method_version='first_order_v1'` AND the JOIN to `studies.reporting_currency = 'AUD'` returns > 0 rows. Otherwise B.

---

## 5. Global acceptance criteria

All of the following must hold before merge:

- [ ] `pytest tests/test_revaluation_math.py tests/test_revaluation_pipeline.py tests/test_revaluation_prices.py -v` → 25/25 green (22 existing + 3 new).
- [ ] `pytest` (full suite) → no regressions.
- [ ] `mypy --strict revaluation/` → clean.
- [ ] `ruff check revaluation/ tests/test_revaluation_*.py` → clean.
- [ ] `grep -rn "first_order_v1" revaluation/` → returns nothing (all bumped).
- [ ] `grep -rn "fx_divisor" revaluation/` → returns nothing (dead code removed).
- [ ] `grep -rn "AUD/USD\|already in AUD" revaluation/` → returns nothing (old ambiguous comments removed).
- [ ] Manual smoke: `python -m scripts.run_revaluation_poc DEG` against a fixture AUD-reporting DFS produces an `npv_spot > npv_dfs` row with `method_version='first_order_v2'`.

---

## 6. Out of scope (track separately)

- Renaming `price_dfs_usd` → `price_dfs_native` in the dataclass (breaking change; pipeline.py call site updates; defer).
- Adding multi-currency support beyond USD/AUD (e.g., EUR for Vulcan).
- Second-order corrections (price-sensitive AISC adjustments, ramp-up modeling, sustaining-capex repricing).
- Revaluing the cash-flow profile with explicit per-year production schedule (currently flat-annuity approximation).
- Frontend display of `method_version` and FX rate alongside revaluation numbers.
