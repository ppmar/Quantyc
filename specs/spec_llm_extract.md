# SPEC: DFS Revaluation Pipeline (Proof-of-Concept)

> Audience: Claude Code.
> Goal: End-to-end proof-of-concept that extracts a DFS, fetches current spot prices, and recomputes NPV at spot — surfacing the difference as a decision signal.
> Scope: **Gold and Copper only** (commodities where Yahoo Finance has reliable spot data). Other commodities are out of scope for this iteration.
> Builds on `SPEC_dfs_parser_gemini.md` — that SPEC must ship first.

## Strategic context

A DFS publishes economic outputs (NPV, IRR) based on price assumptions frozen at publication time. A gold DFS published in June 2023 at $1900/oz USD assumes that price for its entire mine life. Spot gold today (May 2026) is dramatically different, but the market often still prices the stock against the original NPV. This is the inefficiency the project targets.

The math is first-order: we approximate `NPV_spot ≈ NPV_DFS + ΔPrice × Production × Annuity_factor`. It's not a full DCF rerun, and that's intentional — a full rerun requires production curves, capex phasing, and tax structures that DFS executive summaries don't expose. The first-order approximation is sufficient as a decision signal for "is the market pricing this stock against stale assumptions?"

## Out of scope (explicit)

These belong to future SPECs, not this one:

- Lithium, uranium, REE, nickel, zinc, iron ore. Yahoo Finance doesn't have reliable spot for these; they require paid sources (Fastmarkets, UxC, Platts).
- Full DCF re-modeling with year-by-year production curves.
- Cost inflation modeling (capex/opex sensitivity to commodity prices).
- Multi-commodity polymetallic deposits (POC handles single primary commodity).
- FX revaluation (FX exposure is real but second-order vs price exposure).
- Tax structure modeling beyond a flat effective rate.

## Inviolable constraints

- **Au and Cu only.** Detection logic must skip and log if `primary_commodity` is anything else. Don't fail silently with wrong math.
- **Pure functions for the math.** Revaluation logic is testable in isolation with hardcoded inputs. No DB calls, no network calls inside math functions.
- **Idempotent calculations.** Same DFS + same spot price → same output, every time. Use `Decimal` not `float` for monetary math.
- **Price provenance preserved.** Every revaluation row records the spot price used, the timestamp it was fetched, the source. Audit trail matters.
- **Failure modes are explicit.** Missing `annual_production`, missing `recovery_pct`, unknown commodity, stale spot price — each is a distinct, named failure that prevents the row from being written.

## The math (formal)

Given a DFS extraction with:
- `P_dfs` = price assumption from DFS (in USD per oz or per lb)
- `NPV_dfs` = post-tax NPV reported in DFS (in millions of reporting_currency)
- `Q` = annual production (in oz for Au, in lb or t for Cu)
- `Q_unit` = production unit
- `r` = recovery percentage (already factored into Q for most DFS — see notes)
- `R` = discount rate (e.g., 0.08 for 8%)
- `T` = mine life in years
- `t` = effective tax rate (default 0.30, configurable per jurisdiction later)
- `FX` = AUD/USD or USD/AUD rate at DFS date (1.0 if same currency)

And a spot price:
- `P_spot` = current spot price (same unit as P_dfs)

Annuity factor:
```
A(R, T) = (1 - (1 + R)^(-T)) / R
```

Annual revenue uplift (in USD):
```
ΔRevenue_USD = Q × (P_spot - P_dfs)
```

NPV uplift in reporting_currency millions:
```
ΔNPV = (ΔRevenue_USD × A(R, T) × (1 - t)) / 1_000_000 × FX_factor
```

Revalued NPV:
```
NPV_spot = NPV_dfs + ΔNPV
```

Signal strength (used for sorting/filtering):
```
uplift_pct = (NPV_spot - NPV_dfs) / NPV_dfs
```

### Notes on production and recovery

DFS executive summaries usually report **payable annual production** — the metal actually sold, already net of recovery losses. In that case, `r` is informational (don't double-apply it). If the DFS reports "mill throughput" instead (rare in exec summary, common in process flow sheets), recovery would need to be applied. The Gemini prompt must be explicit: extract **payable annual production** if reported, mill throughput only as fallback with a warning.

### Notes on FX

A DFS reporting in AUD with gold price assumption in USD has implicit FX exposure. For Au/Cu, the standard is to express both `P_dfs` and `P_spot` in USD, then convert `ΔNPV` to reporting_currency using a single AUD/USD rate. Use the FX rate at the time of revaluation (current FX), not at DFS date — this is what an analyst actually does. Yahoo Finance gives `AUDUSD=X`.

## Schema changes

Two new tables, one column added.

### New: `commodity_prices`

```sql
CREATE TABLE IF NOT EXISTS commodity_prices (
    price_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity       TEXT    NOT NULL,         -- 'Au', 'Cu', 'AUDUSD' (FX as pseudo-commodity)
    price_usd       REAL    NOT NULL,         -- numeric value
    unit            TEXT    NOT NULL,         -- 'USD/oz', 'USD/lb', 'AUD/USD'
    source          TEXT    NOT NULL,         -- 'yahoo:GC=F', 'yahoo:HG=F', 'manual'
    fetched_at      TEXT    NOT NULL          -- ISO timestamp
);

CREATE INDEX IF NOT EXISTS idx_prices_commodity_time
    ON commodity_prices(commodity, fetched_at DESC);
```

This is append-only. Latest price for a commodity = `ORDER BY fetched_at DESC LIMIT 1`.

### New: `revaluations`

```sql
CREATE TABLE IF NOT EXISTS revaluations (
    revaluation_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    study_id                INTEGER NOT NULL REFERENCES studies(study_id),
    project_id              INTEGER NOT NULL REFERENCES projects(project_id),
    company_id              INTEGER NOT NULL REFERENCES companies(company_id),
    computed_at             TEXT    NOT NULL,
    commodity               TEXT    NOT NULL,
    price_dfs               REAL    NOT NULL,   -- DFS assumption, in USD
    price_spot              REAL    NOT NULL,   -- spot used, in USD
    price_spot_id           INTEGER NOT NULL REFERENCES commodity_prices(price_id),
    fx_rate                 REAL,                -- AUD/USD or other; NULL if reporting_currency='USD'
    fx_rate_price_id        INTEGER REFERENCES commodity_prices(price_id),
    annual_production       REAL    NOT NULL,
    annual_production_unit  TEXT    NOT NULL,
    mine_life_years         REAL    NOT NULL,
    discount_rate_pct       REAL    NOT NULL,
    tax_rate_pct            REAL    NOT NULL,
    annuity_factor          REAL    NOT NULL,   -- computed and stored for audit
    npv_dfs                 REAL    NOT NULL,   -- as reported, in reporting_currency millions
    npv_spot                REAL    NOT NULL,   -- recomputed, in reporting_currency millions
    npv_uplift              REAL    NOT NULL,   -- npv_spot - npv_dfs
    npv_uplift_pct          REAL    NOT NULL,   -- (npv_spot - npv_dfs) / npv_dfs
    method_version          TEXT    NOT NULL,   -- 'first_order_v1' for this SPEC
    warnings                TEXT                -- JSON list of warning strings
);

CREATE INDEX IF NOT EXISTS idx_revaluations_company
    ON revaluations(company_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_revaluations_uplift
    ON revaluations(npv_uplift_pct DESC);
```

### Modified: `studies`

Add one column (the DFS parser already populates `discount_rate_pct` via migration 0004; we add the tax rate now):

```sql
ALTER TABLE studies ADD COLUMN tax_rate_pct REAL;
```

Migration file: `db/migrations/0005_revaluation_tables.sql`

## Files added

```
parsers/dfs_study_prompt.md            # NEW: LLM instructions doc (the one Younes asked for)
revaluation/__init__.py                # NEW: revaluation package
revaluation/math.py                    # NEW: pure-function math layer
revaluation/prices.py                  # NEW: Yahoo Finance fetcher + cache
revaluation/pipeline.py                # NEW: orchestrates extraction → price fetch → compute → persist
scripts/run_revaluation_poc.py         # NEW: CLI for one-shot end-to-end demo
tests/test_revaluation_math.py         # NEW: math unit tests, hardcoded values
tests/test_revaluation_prices.py       # NEW: price fetcher tests with mocked HTTP
tests/test_revaluation_pipeline.py     # NEW: integration test on the POC fixture
db/migrations/0005_revaluation_tables.sql
```

## Files modified

```
parsers/dfs_study.py                   # load prompt from external .md file instead of inline
parsers/dfs_study_schemas.py           # add tax_rate_pct (optional) and tighten annual_production semantics
requirements.txt                       # no changes — yfinance is optional, use raw requests
```

## Implementation by layer

### Layer 1 — The LLM instructions doc (`parsers/dfs_study_prompt.md`)

A standalone markdown file. The Python code reads it at runtime and passes its content as the system prompt to Gemini. This makes prompt iteration a non-code change.

Content structure:

```markdown
# DFS Extraction Instructions for LLM

You are extracting economic parameters from a Definitive Feasibility Study (DFS)
published by an ASX-listed mining company. The downstream system reproduces the DFS's
NPV calculation at current commodity prices to detect market mispricing.

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
by multiplying throughput × grade × recovery — the downstream system will not trust
the result.

## Critical: discount rate is mandatory

DFS announcements always state their discount rate, usually as "NPV8" (8%), "NPV at 5%",
"NPV5%", etc. Some announcements show NPVs at multiple discount rates — extract the
post-tax NPV at the rate explicitly chosen as the "base case" or "preferred" or
"headline" by the company. If unclear, prefer 8% (the industry default).

## Critical: distinguish post-tax from pre-tax NPV

Both fields exist in DFSExtraction. Populate each only with its specific value.
If only one is stated, leave the other null. NEVER infer one from the other.

## Currency handling

- `reporting_currency` is the currency of the NPV.
- All monetary fields (NPV, capex, opex_per_unit when in $/t terms) must be in that currency.
- For commodity prices in `price_assumptions`: use the price as quoted in the DFS,
  typically USD/oz for gold or USD/lb for copper, REGARDLESS of reporting_currency.
- For FX assumption: extract if explicitly stated in the assumptions table.

## Tax rate

Look for "effective tax rate", "company tax rate", or "royalty + tax". Common values:
- Australian gold: 30% corporate + 2.5% state royalty ≈ 32.5%
- Australian copper: 30% corporate + state royalty (varies 2.7-5%)
- Burkina Faso gold (relevant for WAF): around 28-32% including royalties
- If not stated, leave `tax_rate_pct` null. The downstream system applies 30% default
  and logs a warning.

## Production unit normalization

- For gold: `oz` (troy ounces). NOT `kg`, NOT `g`, NOT `tonnes`.
- For copper: `t` (metric tonnes of contained copper). NOT `lb` for annual production
  even though the price is in USD/lb.

## When in doubt

Use null. Add a warning to `extraction_warnings` explaining what was unclear. The
downstream system handles nulls explicitly; it cannot recover from wrong values.
```

### Layer 2 — Math layer (`revaluation/math.py`)

```python
"""
Pure-function math for first-order DFS revaluation at current spot prices.

NO database access, NO network calls, NO state. All inputs explicit.
Tested with hardcoded values in tests/test_revaluation_math.py.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

METHOD_VERSION = "first_order_v1"
DEFAULT_TAX_RATE = Decimal("0.30")

SUPPORTED_COMMODITIES = {"Au", "Cu"}


@dataclass(frozen=True)
class RevaluationInput:
    commodity: str
    price_dfs_usd: Decimal           # USD per oz (Au) or per lb (Cu)
    price_spot_usd: Decimal          # same unit as price_dfs_usd
    annual_production: Decimal       # in oz (Au) or in tonnes contained Cu
    annual_production_unit: str      # 'oz' or 't'
    mine_life_years: Decimal
    discount_rate_pct: Decimal       # e.g., Decimal("8.0") for 8%
    tax_rate_pct: Optional[Decimal]  # None falls back to DEFAULT_TAX_RATE
    npv_dfs: Decimal                 # in reporting_currency millions
    reporting_currency: str          # 'AUD', 'USD', etc.
    fx_rate: Optional[Decimal]       # rate to convert USD ΔNPV → reporting_currency
                                     # for AUD reporting + USD prices: this is AUD per USD


@dataclass(frozen=True)
class RevaluationResult:
    annuity_factor: Decimal
    npv_dfs: Decimal
    npv_spot: Decimal
    npv_uplift: Decimal
    npv_uplift_pct: Decimal
    delta_revenue_annual_usd: Decimal
    delta_npv_reporting_currency: Decimal
    tax_rate_used: Decimal
    method_version: str
    warnings: list[str]


class RevaluationError(ValueError):
    """Inputs are invalid for revaluation."""


def annuity_factor(discount_rate_pct: Decimal, mine_life_years: Decimal) -> Decimal:
    """Standard annuity factor: A = (1 - (1+r)^-n) / r."""
    if discount_rate_pct <= 0:
        raise RevaluationError(f"discount_rate_pct must be positive, got {discount_rate_pct}")
    if mine_life_years <= 0:
        raise RevaluationError(f"mine_life_years must be positive, got {mine_life_years}")
    r = discount_rate_pct / Decimal("100")
    n = mine_life_years
    one_plus_r = Decimal("1") + r
    # (1+r)^-n via Decimal-aware exponentiation
    factor = (Decimal("1") - one_plus_r ** (-n)) / r
    return factor.quantize(Decimal("0.0001"))


def normalize_production_to_unit_price_basis(
    annual_production: Decimal,
    production_unit: str,
    price_unit_basis: str,
    commodity: str,
) -> tuple[Decimal, list[str]]:
    """
    Reconcile production unit with price unit.

    Gold: production in oz, price in USD/oz → no conversion.
    Copper: production typically in 't' (contained Cu tonnes), price in USD/lb.
            Convert tonnes → lb: 1 t = 2204.62262 lb.
    """
    warnings = []
    if commodity == "Au":
        if production_unit != "oz":
            raise RevaluationError(
                f"Au production must be in 'oz', got '{production_unit}'. "
                f"Check DFS extraction."
            )
        return annual_production, warnings
    elif commodity == "Cu":
        if production_unit == "t":
            # Convert to lb to match price unit
            converted = annual_production * Decimal("2204.62262")
            warnings.append(f"converted_production_{annual_production}t_to_{converted}lb")
            return converted, warnings
        elif production_unit == "lb":
            return annual_production, warnings
        else:
            raise RevaluationError(
                f"Cu production must be in 't' or 'lb', got '{production_unit}'"
            )
    else:
        raise RevaluationError(f"unsupported_commodity:{commodity}")


def revalue(input: RevaluationInput) -> RevaluationResult:
    """First-order revaluation at spot. See SPEC math section."""
    warnings: list[str] = []

    if input.commodity not in SUPPORTED_COMMODITIES:
        raise RevaluationError(f"unsupported_commodity:{input.commodity}")

    tax_rate = input.tax_rate_pct if input.tax_rate_pct is not None else DEFAULT_TAX_RATE * 100
    if input.tax_rate_pct is None:
        warnings.append(f"tax_rate_defaulted_to_{DEFAULT_TAX_RATE * 100}pct")

    # Normalize production units to match price unit basis
    price_unit_basis = "oz" if input.commodity == "Au" else "lb"
    normalized_production, conv_warnings = normalize_production_to_unit_price_basis(
        input.annual_production,
        input.annual_production_unit,
        price_unit_basis,
        input.commodity,
    )
    warnings.extend(conv_warnings)

    # Annual revenue uplift in USD
    delta_price_usd = input.price_spot_usd - input.price_dfs_usd
    delta_revenue_annual_usd = normalized_production * delta_price_usd

    # Annuity factor
    a = annuity_factor(input.discount_rate_pct, input.mine_life_years)

    # ΔNPV in USD, then convert to reporting_currency millions
    delta_npv_usd = delta_revenue_annual_usd * a * (Decimal("1") - tax_rate / Decimal("100"))
    delta_npv_usd_millions = delta_npv_usd / Decimal("1000000")

    if input.reporting_currency == "USD":
        fx_factor = Decimal("1")
    elif input.reporting_currency == "AUD":
        if input.fx_rate is None:
            raise RevaluationError("fx_rate_required_for_aud_reporting_with_usd_prices")
        # fx_rate convention: AUD per USD (so AUD value = USD value × fx_rate when fx_rate > 1)
        fx_factor = input.fx_rate
    else:
        raise RevaluationError(f"unsupported_reporting_currency:{input.reporting_currency}")

    delta_npv_reporting_currency = delta_npv_usd_millions * fx_factor

    npv_spot = input.npv_dfs + delta_npv_reporting_currency
    npv_uplift = npv_spot - input.npv_dfs
    npv_uplift_pct = (npv_uplift / input.npv_dfs) if input.npv_dfs != 0 else Decimal("0")

    return RevaluationResult(
        annuity_factor=a,
        npv_dfs=input.npv_dfs,
        npv_spot=npv_spot.quantize(Decimal("0.01")),
        npv_uplift=npv_uplift.quantize(Decimal("0.01")),
        npv_uplift_pct=npv_uplift_pct.quantize(Decimal("0.0001")),
        delta_revenue_annual_usd=delta_revenue_annual_usd.quantize(Decimal("0.01")),
        delta_npv_reporting_currency=delta_npv_reporting_currency.quantize(Decimal("0.01")),
        tax_rate_used=tax_rate,
        method_version=METHOD_VERSION,
        warnings=warnings,
    )
```

### Layer 3 — Price fetcher (`revaluation/prices.py`)

```python
"""
Yahoo Finance spot price fetcher for Au, Cu, and AUD/USD FX.

Uses the unofficial query2.finance.yahoo.com endpoint via requests.
No yfinance dependency — that library is heavyweight and we only need quotes.
"""
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Symbol → (commodity_code, unit, multiplier_to_canonical_unit)
SYMBOL_MAP = {
    "GC=F":     ("Au", "USD/oz", Decimal("1")),       # gold futures, front month
    "HG=F":     ("Cu", "USD/lb", Decimal("1")),       # copper futures, front month
    "AUDUSD=X": ("AUDUSD", "AUD/USD", Decimal("1")),  # AUD per 1 USD
}

CACHE_TTL_HOURS = 1  # spot prices fresh within last hour are reused


class PriceFetchError(Exception):
    pass


def fetch_yahoo_quote(symbol: str) -> Decimal:
    """Single quote lookup. Raises on any failure — no silent fallback."""
    url = "https://query2.finance.yahoo.com/v7/finance/quote"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Quantyc/1.0)",
        "Accept": "application/json",
    }
    params = {"symbols": symbol}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        raise PriceFetchError(f"yahoo_http_error:{type(e).__name__}:{e}")

    results = data.get("quoteResponse", {}).get("result", [])
    if not results:
        raise PriceFetchError(f"yahoo_empty_result:{symbol}")

    price = results[0].get("regularMarketPrice")
    if price is None:
        raise PriceFetchError(f"yahoo_no_price:{symbol}")

    return Decimal(str(price))


def get_or_fetch_price(conn: sqlite3.Connection, commodity: str) -> tuple[Decimal, int]:
    """
    Returns (price, price_id). Uses cache if a price was fetched within CACHE_TTL_HOURS.
    Otherwise fetches fresh from Yahoo and inserts into commodity_prices.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=CACHE_TTL_HOURS)).isoformat()
    cached = conn.execute(
        """SELECT price_id, price_usd FROM commodity_prices
           WHERE commodity = ? AND fetched_at >= ?
           ORDER BY fetched_at DESC LIMIT 1""",
        (commodity, cutoff),
    ).fetchone()

    if cached:
        return Decimal(str(cached[1])), cached[0]

    # Resolve commodity → symbol
    symbol = None
    unit = None
    for sym, (com, u, _) in SYMBOL_MAP.items():
        if com == commodity:
            symbol = sym
            unit = u
            break
    if symbol is None:
        raise PriceFetchError(f"no_yahoo_symbol_for_commodity:{commodity}")

    price = fetch_yahoo_quote(symbol)
    source = f"yahoo:{symbol}"

    cur = conn.execute(
        """INSERT INTO commodity_prices (commodity, price_usd, unit, source, fetched_at)
           VALUES (?, ?, ?, ?, ?)""",
        (commodity, float(price), unit, source, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    return price, cur.lastrowid
```

### Layer 4 — Pipeline (`revaluation/pipeline.py`)

```python
"""
Orchestrates: study row → price fetch → math → persist to revaluations.

Called by scripts/run_revaluation_poc.py and (later) by the orchestrator on
each new DFS parsed.
"""
import json
import logging
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from revaluation.math import (
    RevaluationInput,
    RevaluationResult,
    SUPPORTED_COMMODITIES,
    METHOD_VERSION,
    revalue,
    RevaluationError,
)
from revaluation.prices import get_or_fetch_price, PriceFetchError

logger = logging.getLogger(__name__)


def revalue_study(conn: sqlite3.Connection, study_id: int) -> Optional[int]:
    """
    Revalue a single study row. Returns revaluation_id on success, None on skip.
    Raises RevaluationError or PriceFetchError on hard failure.
    """
    study = conn.execute("""
        SELECT s.study_id, s.project_id, s.mine_life_years, s.annual_production,
               s.recovery_pct, s.post_tax_npv, s.discount_rate_pct, s.tax_rate_pct,
               s.assumed_price_deck, s.reporting_currency, s.study_stage,
               p.project_id, p.company_id,
               pc.commodity, pc.is_primary
        FROM studies s
        JOIN projects p ON p.project_id = s.project_id
        LEFT JOIN project_commodities pc ON pc.project_id = p.project_id AND pc.is_primary = 1
        WHERE s.study_id = ?
    """, (study_id,)).fetchone()

    if not study:
        raise RevaluationError(f"study_not_found:{study_id}")

    commodity = study["commodity"]
    if commodity not in SUPPORTED_COMMODITIES:
        logger.info("Skipping study %d: commodity %s not supported by POC", study_id, commodity)
        return None

    # Required fields for math
    required = {
        "annual_production": study["annual_production"],
        "mine_life_years": study["mine_life_years"],
        "discount_rate_pct": study["discount_rate_pct"],
        "post_tax_npv": study["post_tax_npv"],
        "reporting_currency": study["reporting_currency"],
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise RevaluationError(f"missing_fields:{','.join(missing)}")

    # Production unit hardcoded for POC: oz for Au, t for Cu (per LLM prompt instructions)
    production_unit = "oz" if commodity == "Au" else "t"

    # Extract DFS price assumption for the primary commodity
    price_deck = json.loads(study["assumed_price_deck"] or "[]")
    price_dfs = None
    for entry in price_deck:
        if entry.get("commodity") == commodity:
            price_dfs = Decimal(str(entry["price"]))
            break
    if price_dfs is None:
        raise RevaluationError(f"no_dfs_price_for_commodity:{commodity}")

    # Spot price
    try:
        price_spot, price_spot_id = get_or_fetch_price(conn, commodity)
    except PriceFetchError as e:
        raise RevaluationError(f"spot_fetch_failed:{e}")

    # FX if needed
    fx_rate = None
    fx_price_id = None
    if study["reporting_currency"] == "AUD":
        fx_rate, fx_price_id = get_or_fetch_price(conn, "AUDUSD")
    elif study["reporting_currency"] != "USD":
        raise RevaluationError(f"reporting_currency_not_supported:{study['reporting_currency']}")

    input_obj = RevaluationInput(
        commodity=commodity,
        price_dfs_usd=price_dfs,
        price_spot_usd=price_spot,
        annual_production=Decimal(str(study["annual_production"])),
        annual_production_unit=production_unit,
        mine_life_years=Decimal(str(study["mine_life_years"])),
        discount_rate_pct=Decimal(str(study["discount_rate_pct"])),
        tax_rate_pct=Decimal(str(study["tax_rate_pct"])) if study["tax_rate_pct"] else None,
        npv_dfs=Decimal(str(study["post_tax_npv"])),
        reporting_currency=study["reporting_currency"],
        fx_rate=fx_rate,
    )

    result = revalue(input_obj)

    cur = conn.execute("""
        INSERT INTO revaluations (
            study_id, project_id, company_id, computed_at,
            commodity, price_dfs, price_spot, price_spot_id,
            fx_rate, fx_rate_price_id,
            annual_production, annual_production_unit,
            mine_life_years, discount_rate_pct, tax_rate_pct, annuity_factor,
            npv_dfs, npv_spot, npv_uplift, npv_uplift_pct,
            method_version, warnings
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        study_id, study["project_id"], study["company_id"],
        datetime.now(timezone.utc).isoformat(),
        commodity, float(price_dfs), float(price_spot), price_spot_id,
        float(fx_rate) if fx_rate else None, fx_price_id,
        float(input_obj.annual_production), production_unit,
        float(input_obj.mine_life_years), float(input_obj.discount_rate_pct),
        float(result.tax_rate_used), float(result.annuity_factor),
        float(result.npv_dfs), float(result.npv_spot),
        float(result.npv_uplift), float(result.npv_uplift_pct),
        result.method_version,
        json.dumps(result.warnings),
    ))
    conn.commit()
    return cur.lastrowid
```

### Layer 5 — CLI (`scripts/run_revaluation_poc.py`)

```python
"""
Run the full revaluation pipeline on one ticker as proof-of-concept.

Steps:
1. Find the most recent DFS for the ticker (in studies, study_stage='DFS')
2. Fetch current spot price (cached if recent)
3. Compute revaluation
4. Print human-readable summary

Usage:
    python -m scripts.run_revaluation_poc DEG
    python -m scripts.run_revaluation_poc DEG --study-id 42  # specific study
"""
```

Output target:

```
Ticker:           DEG
Project:          Hemi
Study:            DFS at 2024-06-15

DFS assumption:   1900.00 USD/oz Au
Spot price:       3520.00 USD/oz Au  (fetched 2026-05-13 14:22 UTC, yahoo:GC=F)
Price uplift:     +85.3%

FX rate:          1.55 AUD/USD  (yahoo:AUDUSD=X)

Annual production:   180,000 oz Au
Mine life:           10.0 years
Discount rate:       5.0%
Tax rate:            30.0% (defaulted — DFS did not state)
Annuity factor:      7.7217

NPV (DFS, AUD M):    985.00
NPV uplift (AUD M):  +1547.83
NPV (spot, AUD M):   2532.83

Signal:           +157.1% uplift to NPV at current spot
                  Market may not have repriced this stock against current commodity prices.

Revaluation row:  #1 (study_id=42, computed_at=2026-05-13T14:22:31Z)
```

## Tests

### `tests/test_revaluation_math.py` — hardcoded math

Critical tests. All values hand-checked.

```python
def test_annuity_factor_10y_8pct():
    """Standard textbook: A(8%, 10) = 6.7101"""
    result = annuity_factor(Decimal("8.0"), Decimal("10"))
    assert abs(result - Decimal("6.7101")) < Decimal("0.0001")

def test_annuity_factor_15y_5pct():
    """A(5%, 15) = 10.3797"""
    result = annuity_factor(Decimal("5.0"), Decimal("15"))
    assert abs(result - Decimal("10.3797")) < Decimal("0.0001")

def test_revalue_gold_aud_reporting():
    """Hand-computed scenario: Hemi-like DFS reuvalued at 3500 USD/oz."""
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"),    # oz
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"),
        npv_dfs=Decimal("985"),                  # AUD M
        reporting_currency="AUD",
        fx_rate=Decimal("1.55"),                 # AUD per USD
    )
    result = revalue(inp)
    # ΔPrice = 1600 USD/oz
    # ΔRev_annual = 180000 × 1600 = 288,000,000 USD/year
    # A(5%, 10) = 7.7217
    # ΔNPV_USD = 288,000,000 × 7.7217 × 0.70 / 1,000,000 = 1556.34 USD M
    # ΔNPV_AUD = 1556.34 × 1.55 = 2412.32 AUD M
    # NPV_spot = 985 + 2412.32 = 3397.32 AUD M
    assert abs(result.npv_spot - Decimal("3397.32")) < Decimal("1.0")

def test_revalue_copper_usd_reporting():
    """Copper case: production in tonnes, price in USD/lb."""
    inp = RevaluationInput(
        commodity="Cu",
        price_dfs_usd=Decimal("3.50"),         # USD/lb
        price_spot_usd=Decimal("4.80"),         # USD/lb
        annual_production=Decimal("25000"),     # tonnes
        annual_production_unit="t",
        mine_life_years=Decimal("15"),
        discount_rate_pct=Decimal("8.0"),
        tax_rate_pct=Decimal("30.0"),
        npv_dfs=Decimal("450"),                 # USD M
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    # 25000 t × 2204.62262 lb/t = 55,115,565 lb
    # ΔRev = 55,115,565 × 1.30 = 71,650,234 USD/year
    # A(8%, 15) = 8.5595
    # ΔNPV = 71,650,234 × 8.5595 × 0.70 / 1,000,000 = 429.31 USD M
    assert abs(result.npv_spot - Decimal("879.31")) < Decimal("2.0")

def test_unsupported_commodity_raises():
    inp = RevaluationInput(
        commodity="Li2O",
        price_dfs_usd=Decimal("1500"),
        price_spot_usd=Decimal("800"),
        annual_production=Decimal("500000"),
        annual_production_unit="t",
        mine_life_years=Decimal("20"),
        discount_rate_pct=Decimal("8"),
        tax_rate_pct=None,
        npv_dfs=Decimal("2000"),
        reporting_currency="AUD",
        fx_rate=Decimal("1.55"),
    )
    with pytest.raises(RevaluationError, match="unsupported_commodity"):
        revalue(inp)

def test_au_production_wrong_unit_raises():
    inp = ...  # Au with annual_production_unit="kg"
    with pytest.raises(RevaluationError, match="Au production must be in 'oz'"):
        revalue(inp)

def test_zero_dfs_price_no_division_by_zero():
    """Edge case: DFS reports NPV but price assumption was missing/zero."""
    # ...

def test_negative_price_change_lowers_npv():
    """Sanity: if spot < DFS price, NPV decreases."""
    # ...
```

### `tests/test_revaluation_prices.py` — mocked HTTP

```python
@patch("revaluation.prices.requests.get")
def test_fetch_yahoo_quote_gold(mock_get):
    mock_get.return_value.json.return_value = {
        "quoteResponse": {"result": [{"symbol": "GC=F", "regularMarketPrice": 3520.5}]}
    }
    mock_get.return_value.raise_for_status = lambda: None
    result = fetch_yahoo_quote("GC=F")
    assert result == Decimal("3520.5")

def test_fetch_yahoo_quote_empty_result_raises(mock_get):
    mock_get.return_value.json.return_value = {"quoteResponse": {"result": []}}
    # ...
    with pytest.raises(PriceFetchError, match="yahoo_empty_result"):
        fetch_yahoo_quote("GC=F")

def test_get_or_fetch_uses_cache_within_ttl(mock_get, in_memory_db):
    # Insert a fresh price
    # Call get_or_fetch_price
    # Assert mock_get NOT called, returned value matches inserted
    # ...

def test_get_or_fetch_calls_yahoo_when_stale(mock_get, in_memory_db):
    # Insert a stale price (older than CACHE_TTL_HOURS)
    # Call get_or_fetch_price
    # Assert mock_get called, new row inserted
    # ...
```

### `tests/test_revaluation_pipeline.py` — integration

```python
def test_revalue_study_end_to_end_au(in_memory_db, mock_yahoo):
    """Insert a synthetic DFS study row for a gold project, run revalue_study, verify revaluations row."""
    # Setup: insert companies, projects, project_commodities (Au), studies (DFS)
    # Mock Yahoo to return spot 3520 USD/oz and AUD/USD 1.55
    # Call revalue_study(conn, study_id)
    # Query revaluations row, assert values match hand-computed result
    # Assert warnings = []

def test_revalue_study_skips_lithium():
    """Li2O project: revalue_study returns None, no row inserted, info logged."""
    # ...

def test_revalue_study_raises_on_missing_npv():
    """Study with NULL post_tax_npv: raises RevaluationError('missing_fields:post_tax_npv')."""
    # ...
```

## Sequenced PRs

This is "tout-en-un" but with internal layering — each PR is independently testable.

### PR 1 — Math layer + tests
- `revaluation/math.py` complete
- `tests/test_revaluation_math.py` with all hand-computed assertions
- No DB, no network, no Pydantic dependencies
- Mergeable as a standalone math module

### PR 2 — Schema + prices module
- Migration `0005_revaluation_tables.sql`
- `revaluation/prices.py` with Yahoo fetcher + SQLite cache
- `tests/test_revaluation_prices.py` with mocked HTTP
- Pre-merge: run `python -c "from revaluation.prices import fetch_yahoo_quote; print(fetch_yahoo_quote('GC=F'))"` and paste in PR description

### PR 3 — DFS prompt externalized + Pydantic update
- Move inline `EXTRACTION_PROMPT` from `parsers/dfs_study.py` to `parsers/dfs_study_prompt.md`
- Load it at runtime: `Path(__file__).parent / "dfs_study_prompt.md"`
- Add `tax_rate_pct` to `DFSExtraction` (optional)
- Add `tax_rate_pct` column to studies via migration
- Update persistence to write the new field
- Tests: existing DFS tests still pass; new test for tax_rate field

### PR 4 — Pipeline + CLI + end-to-end test
- `revaluation/pipeline.py`
- `scripts/run_revaluation_poc.py`
- `tests/test_revaluation_pipeline.py`
- Pre-merge: run on the validated DFS fixture, paste CLI output in PR description

## What success looks like

After PR 4 ships and you have one validated DFS in production:

```bash
$ python -m scripts.run_revaluation_poc <TICKER>
[full output as shown in Layer 5]
```

If that prints sensible numbers with sensible warnings (and the math checks out vs your manual calculation), the POC is done. You can then make decisions about:

1. Whether the signal is interesting enough to surface in the UI (probably yes, in a new "Valuation" or "Signals" tab)
2. Whether to extend to lithium/uranium (different price source story)
3. Whether to improve the math (capex inflation, FX hedging, tax sophistication)

## Inviolable constraints recap

- Au and Cu only. Skip other commodities, never compute with wrong math.
- Decimal everywhere for money. Never float in the math layer.
- Production must be payable. The prompt makes this explicit; the math trusts that contract.
- Spot price provenance tracked. Every revaluation row links to the exact `commodity_prices` row used.
- Method version stored. When the math evolves, old revaluations remain interpretable.
- No vendor lock-in on Yahoo. The price fetcher is one file, ~80 lines, easily swappable for Polygon, Metals-API, or hardcoded values later.
