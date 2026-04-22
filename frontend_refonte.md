# Company Detail Page — Frontend Redesign Spec (v1, scoped)

## Context

Replaces the current AGY/SXG company detail page with an editorial Summary view, scoped strictly to data already in the database (quarterly cashflow + securities quotation extractions).

The page is built so that adding new extraction sources later — option overhang from annual filings, drilling momentum from the exploration parser, resource estimates, holder register — extends the existing layout without rebuilding it. This document defines both the v1 scope and the extension contract.

---

## Design principles

1. **No regulatory jargon in the UI.** The user sees `Placement`, `Quarterly update`, `Options exercised` — never `2A`, `5B`, `Appendix 3B`. The platform is an analysis layer, not a filings viewer. Translation from filing type → product label happens in the backend.
2. **Editorial over dashboard.** Two large anchor numbers, prose context, prose activity feed. No KPI tile grids. Hierarchy is established by typography (size, weight, letter-spacing), not by box-inside-box card nesting.
3. **Monochrome plus a single accent.** Page is built on a zinc neutral scale. The brand amber (`#f5b642`) appears only on the ticker label and on capital event markers in the chart. No category colour-coding for event types.
4. **Tabular numerics, sans-serif everywhere.** Numbers use `font-variant-numeric: tabular-nums`; no monospace font. The "Bloomberg terminal" aesthetic is explicitly rejected.
5. **Hide what we don't have.** When optional data is unavailable, the relevant line, tab, or section is omitted entirely. Never `—` placeholders, never `not yet extracted` text in primary content.
6. **Pure projection of the snapshot.** The frontend does not compute runway, deltas, dilution, or labels. The backend returns display-ready strings and pre-computed values.

---

## Layout (matches editorial mockup)

The layout below uses concrete sample values for legibility. It is the canonical layout for **every covered ticker** — there is no per-ticker variant. Companies in the universe today span ~10+ ASX juniors across lithium, gold, rare earths, uranium, and base metals; the page renders identically for all of them, with the snapshot data driving the content.

```
←  Dashboard

XYZ
Sample Mining Limited
Commodity · flagship project · jurisdiction

────────────────────────────────────────────────────
Summary    Financials    Capital    Documents
────────────────────────────────────────────────────

CASH POSITION                CAPITAL STRUCTURE

A$30.2M                      182.4M
~8 quarters of runway        shares on issue

Burn A$3.75M per quarter,    Last issuance 4.2M shares
up from A$3.12M prior.       45 days ago at A$3.20.
Treasury 31 Dec 2025.

────────────────────────────────────────────────────

CASH TRAJECTORY                              8 quarters

[ line chart, with placement marker ]

────────────────────────────────────────────────────

RECENT ACTIVITY

Placement                              45 days ago
A$13.4M raised at A$3.20 per share. 4.2M new shares.

Quarterly update                       18 days ago
Closed at A$30.2M cash. Burn A$3.75M. Guidance A$4.1M.

Options exercised                      28 days ago
0.8M shares issued at A$1.50 strike.

View all activity →
```

The right hero column intentionally shows only what the current data supports. When option/rights tracking lands, that column gains lines; the layout does not change.

---

## Tabs

Five tabs are reserved in the navigation. Their visibility is data-driven:

| Tab | v1 status | Visible when |
|---|---|---|
| Summary | active | always |
| Financials | active | quarterly cashflow data exists (≥1 quarter) |
| Capital | active | securities quotation data exists (≥1 issuance) |
| Operations | hidden in v1 | exploration / resource / production data exists |
| Documents | active | any classified announcement exists |
| Holders | hidden in v1 | top-20 / substantial holders data exists |

A hidden tab does not occupy space — it is not rendered at all. Adding the data source later automatically reveals the tab. The Summary tab is the page described above; this spec covers Summary only. The other tabs are stubs in v1, defined in later specs.

---

## Coverage states

The covered universe spans tickers at very different points of pipeline maturity. The page must render with dignity at every point on the spectrum, not just on well-covered pilots.

| State | Trigger | Render |
|---|---|---|
| Loading | snapshot request in flight | Skeleton in the exact shape of the populated layout. No spinners. |
| Empty | snapshot returns `has_data: false` (ticker is registered, no documents parsed yet) | Full-page state: company header renders, then a single line `"Documents are being processed for this ticker"` in muted text. No tabs. |
| Sparse | snapshot has `cash` but `cash_history.length < 3` and `activity.length === 0` | Hero left renders normally. Hero right hidden if `capital.shares_display` is absent. Chart hidden. Activity feed hidden. Tabs visible per their own rules. |
| Partial | snapshot has cash + capital but partial history (3–7 quarters) or no marked capital events | Full layout renders. Chart shows whatever history exists. Activity feed shows whatever events exist. |
| Populated | snapshot has cash + capital + 8 quarters + multiple activity events | Full layout as in the mockup. |
| Error | snapshot fetch fails | Section-level error inside the page shell with a retry; never a blank page. |

The `Sparse` and `Partial` states matter because most newly added tickers will land there before reaching `Populated`. The page should look intentional in those states, not broken.

---

## Component inventory

### `<CompanyHeader>`
Ticker (small, accent), full name (h1), one-line meta string (commodity + project hint + jurisdiction).

### `<TabBar>`
Horizontal text links with a 1px underline on the active tab. No background, no pill chrome. Iterates the tab list and skips any tab marked `visible: false` in the snapshot.

### `<HeroGrid>` — two-column
Renders two `<HeroStat>` slots side by side.

### `<HeroStat>`
Props: `label`, `value`, `subtext`, `caption`. The `value` is rendered at 44px / weight 500 / letter-spacing -0.035em. No internal chrome; spacing only.

When the right-side hero gains optional fields (fully diluted, overhang) in a later phase, this same component renders an extra line in `caption` — no new component required.

### `<CashTrajectoryChart>`
Single line chart (Recharts `<AreaChart>` or `<LineChart>` with subtle fill). Markers overlay the line for capital events. Y-axis labels at right edge, X-axis labels at start/middle/end only.

Marker convention:
- Filled white dot = latest data point (state)
- Open amber dot = capital event (event)
- Future event types (e.g., resource update) extend the marker shape vocabulary; the chart accepts an array of `{quarter, type}` markers without code changes.

### `<ActivityFeed>`
Vertical stack of `<ActivityItem>`. Each item is prose: a bold short label on the left, a relative date on the right, a one-line plain-language description below. No icons, no coloured dots, no category badges.

Footer link: `View all activity →`.

### `<ActivityItem>`
Props: `headline` (e.g. `"Placement"`), `relative_date` (e.g. `"45 days ago"`), `detail` (e.g. `"A$13.4M raised at A$3.20 per share. 4.2M new shares."`).

The frontend does not know what regulatory event produced the item. All translation happens upstream.

### Shared
`<Separator>` (1px, rgba white at 0.08), back-arrow link, view-all-arrow link.

---

## Data contract — `/api/company/{ticker}/snapshot`

```typescript
type CompanySnapshot = {
  ticker: string
  name: string
  exchange: 'ASX' | 'TSX'
  meta_line: string                // backend-composed, e.g. "Gold exploration · Project X · Western Australia"
  has_data: boolean                // false → empty state; true → at least cash or capital is present

  cash?: CashSection               // v1, present when at least one quarterly extraction exists
  capital?: CapitalSection         // v1, present when at least one securities issuance has been parsed
  cash_history: CashHistoryPoint[] // v1, may be empty array
  activity: ActivityEvent[]        // v1, may be empty array
  tabs: TabVisibility              // v1, required

  exploration?: ExplorationSection  // v2+, optional
  resource?: ResourceSection        // v2+, optional
  holders?: HoldersSection          // v2+, optional
}

type CashSection = {
  amount_display: string           // "A$30.2M" — backend formats currency and scale
  as_of_display: string            // "31 Dec 2025"
  runway_display: string           // "~8 quarters of runway"
  prose: string                    // "Burn A$3.75M per quarter, up from A$3.12M prior. Treasury 31 Dec 2025."
}

type CapitalSection = {
  // v1 — always present
  shares_display: string           // "182.4M"
  shares_label: string             // "shares on issue"
  prose: string                    // "Last issuance 4.2M shares 45 days ago at A$3.20."

  // v2+ — present once option/rights tracking ships
  fully_diluted_display?: string   // "198.2M"
  overhang_display?: string        // "8.0% option overhang"
  fully_diluted_prose?: string     // additional sentence appended to prose
}

type CashHistoryPoint = {
  quarter: string                  // "Q1 25"
  cash_balance: number             // raw number for chart scale
  marker?: 'placement' | 'options_exercised' | 'resource_update' | 'drilling_result'
}

type ActivityEvent = {
  id: string                       // for keys and click-through
  headline: string                 // "Placement"
  relative_date: string            // "45 days ago"
  detail: string                   // "A$13.4M raised at A$3.20 per share. 4.2M new shares."
  source_url?: string              // link to original announcement, opened on click
}

type TabVisibility = {
  summary: true                    // always
  financials: boolean
  capital: boolean
  operations: boolean              // false in v1
  documents: boolean
  holders: boolean                 // false in v1
}
```

The backend is responsible for:
- Translating filing types into product labels (`2A reason=placement` → headline `"Placement"`)
- Composing prose strings (`"Burn A$3.75M per quarter, up from A$3.12M prior."`)
- Computing relative dates against today
- Formatting currency, share counts, and percentages
- Choosing which events qualify as activity (materiality filter)
- Setting `tabs.*` based on what extractions exist for the ticker
- Composing `meta_line` per ticker (commodity + flagship project + jurisdiction). For the current universe (~10+ tickers and growing), the recommended source is a maintained CSV of `(ticker, commodity, flagship_project, jurisdiction)` enriched as new tickers are added. The official company name comes from the ASX API directly. This avoids depending on a presentation-deck parser for v1.

The frontend renders. It does not compute, format, or label.

---

## Extensibility contract

Adding new data sources later must not require layout changes. The contract:

| New capability | What changes | What the frontend touches |
|---|---|---|
| Option / rights tracking (annual filing parser) | `capital.fully_diluted_display`, `overhang_display`, `fully_diluted_prose` populate | nothing — `<HeroStat>` already renders extra caption lines |
| Drilling momentum (exploration parser) | `exploration` section populates, `tabs.operations = true`, new `activity[]` entries with `headline: "Drilling results"` | nothing for Summary; new `<OperationsTab>` for the tab content |
| Resource estimate (JORC parser) | `resource` populates, marker types extended on `cash_history` | chart accepts the new marker type via lookup table |
| Holder register | `holders` populates, `tabs.holders = true` | new `<HoldersTab>` for the tab content |
| New event categories | new `headline` values appear in `activity[]` | nothing — `<ActivityItem>` is structurally agnostic |

The principle: the frontend's job is to render a typed projection, not to know about the regulatory taxonomy behind it. Every code path that branches on event category or filing type lives in the backend.

---

## Invariants

1. Exactly one API call per page load (`GET /api/company/{ticker}/snapshot`).
2. No mention of any regulatory document type in any user-visible string. All translations happen backend-side.
3. No `—` placeholders anywhere. When `cash`, `capital`, or any optional field is absent, the corresponding section, line, or column is omitted entirely. The empty state (`has_data: false`) renders only the company header and a single muted line.
4. Hidden tabs are not rendered (no greyed-out, no `coming soon`).
5. The `<HeroGrid>` renders one or two columns based on which sections are present. The right column adapts its content to v1 (shares only) or v2+ (shares + fully diluted + overhang) based on optional fields in `capital`. The left column is hidden if `cash` is absent. The right column is hidden if `capital` is absent. If both are absent, the grid does not render.
6. Chart markers are pre-classified by the backend on `cash_history[].marker`. The frontend does not infer which quarter had a placement.
7. Numbers everywhere use `font-variant-numeric: tabular-nums`. No monospace font is used in this page.
8. Colour usage is restricted to neutral zinc + the amber accent. No categorical colour-coding of events. (Later phases may introduce a single warning colour if overhang / runway thresholds are crossed; not in v1.)
9. Skeletons match the final shape — the page does not visibly reflow after data arrives.
10. Every activity item is click-through to its `source_url` if present.

---

## Stack

React 18 · TypeScript · Tailwind CSS · shadcn/ui · Recharts · Lucide.

Component mapping:

| Region | Implementation |
|---|---|
| Layout shell | plain `<div>` with Tailwind spacing |
| Tab bar | shadcn `<Tabs>` with `variant="underline"` (custom) or hand-rolled — shadcn default is too pill-heavy |
| Separators | shadcn `<Separator>` |
| Chart | Recharts `<AreaChart>` with `<Area>`, `<XAxis>`, `<YAxis>`, custom `<ReferenceDot>` for capital event markers |
| Icons | Lucide `ArrowLeft`, `ArrowRight` only |
| Typography | Tailwind tokens: `text-3xl font-medium tracking-tight` for hero numbers, `text-xs uppercase tracking-wider text-zinc-500` for section labels |

Colour tokens (Tailwind config additions):

```ts
colors: {
  bg: '#09090b',           // zinc-950 equivalent
  fg: '#fafafa',           // zinc-50
  muted: '#a1a1aa',        // zinc-400
  subtle: '#71717a',       // zinc-500
  faint: '#52525b',        // zinc-600
  divider: 'rgba(255,255,255,0.08)',
  accent: '#f5b642',       // brand amber, used only on ticker + chart event markers
}
```

`tabular-nums` is set globally on the page wrapper.

---

## File structure

```
src/
├── app/
│   └── company/[ticker]/
│       └── page.tsx                    # orchestrator: one fetch, one snapshot
├── components/
│   └── company/
│       ├── CompanyHeader.tsx
│       ├── TabBar.tsx
│       ├── HeroGrid.tsx
│       ├── HeroStat.tsx
│       ├── CashTrajectoryChart.tsx
│       ├── ActivityFeed.tsx
│       └── ActivityItem.tsx
├── hooks/
│   └── useCompanySnapshot.ts
├── types/
│   └── snapshot.ts                     # CompanySnapshot and sub-types
└── lib/
    └── tokens.ts                       # colour and spacing exports for non-Tailwind use
```

---

## Delivery order

1. **Types + hook + skeleton page** — define `CompanySnapshot` exactly as in this spec, build `useCompanySnapshot`, render skeletons in the final layout shape. No data needed.
2. **CompanyHeader + TabBar + HeroGrid (left column only)** — ships against `cash` data. The right column shows shares only.
3. **HeroGrid right column** — adds shares display from `capital`. Layout already exists from step 2.
4. **CashTrajectoryChart** — line + axis + latest-point marker. Capital event markers added once `cash_history[].marker` is populated.
5. **ActivityFeed + ActivityItem** — renders the prose feed.
6. **Tab routing for Financials / Capital / Documents** — Summary stays default; other tabs are stubs that fetch their own slice. (Out of scope for this spec; defined separately.)

Each step produces a complete, shippable page for the scope it covers.

---

## Test fixtures

Fixtures are defined by **coverage profile**, not by specific ticker. The profiles below should be backed by real production data, picking whichever ticker currently fits each profile (the matching ticker will change over time as parsing coverage improves):

| Profile | Definition | What it tests |
|---|---|---|
| `populated` | cash + capital + ≥6 quarters + ≥3 activity events including ≥1 placement | Full layout renders end-to-end. Chart marker visible. |
| `partial` | cash + capital + 3–5 quarters + 1–2 activity events, no placement marker | Chart and feed degrade gracefully without looking broken. |
| `sparse` | cash present but `cash_history.length < 3` and `activity.length === 0` | Hero left renders, hero right hidden, chart hidden, feed hidden. |
| `capital_only` | capital present but no cash extraction yet | Hero right renders alone. Hero left and chart hidden. |
| `empty` | `has_data: false` | Empty state — header + single muted line, no tabs. |
| `error` | snapshot fetch returns 5xx | Error shell with retry, no blank page. |

When picking the concrete ticker for each profile, prefer one with a high `parsed/docs` ratio for the `populated` profile (so the data is genuinely complete) and a lower-ratio ticker for `partial` and `sparse`. The dashboard's `parsed/docs` column is the right indicator for fixture selection.

Snapshot tests assert (for every profile that should render content):
- The right hero column renders only the v1 lines when `capital.fully_diluted_display` is absent
- `tabs.operations` and `tabs.holders` are absent from rendered DOM in v1
- No string in the rendered page contains `"2A"`, `"5B"`, `"Appendix"`, `"3B"`, `"3G"`, or `"4G"`
- All numeric content nodes inherit `font-variant-numeric: tabular-nums`
- The empty state never renders alongside any tab or chart

A `populated_v2` fixture should be added when option tracking ships, asserting that the optional caption lines render in the right hero column without any layout shift relative to `populated`.
