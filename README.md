# Quantyc

**Financial intelligence for ASX junior miners — from raw regulatory filings to investment signals.**

🔗 **Live app:** [frontend-production-258d.up.railway.app](https://frontend-production-258d.up.railway.app)

---

## The problem

Australia has hundreds of small, listed mining exploration companies. They live and die on two
questions: *how much cash do they have left*, and *what is their deposit actually worth at
today's metal prices?*

The answers exist — but they're buried in thousands of PDF announcements filed with the ASX:
quarterly cashflow reports, capital raisings, drill results, and feasibility studies whose
economics were computed at commodity prices that are often years out of date. No one reads them
all. Quantyc does.

## What it does

Quantyc ingests the raw ASX announcement feed and turns it into a live, queryable picture of the
whole junior-mining universe:

- **Cash & survival** — quarterly cash position, burn rate, and runway for every company,
  extracted straight from the mandated Appendix 5B filings. A pre-revenue explorer with two
  quarters of cash left is about to raise money and dilute shareholders; Quantyc sees it coming.
- **Capital structure** — shares on issue, options, dilution history, reconciled across filing
  types so a wrong number in one document doesn't silently corrupt the picture.
- **Project revaluation** — the flagship. Every feasibility study states an NPV at the metal
  prices of its day. Quantyc extracts the study's economics (production, mine life, price deck,
  tax) and re-computes what the project is worth at **today's spot prices** — gold, silver,
  copper, palladium, platinum, uranium — refreshed daily. A gold project valued at US$1,300/oz
  in its study looks very different at US$4,000/oz.
- **Multi-commodity baskets** — polymetallic projects (copper-gold, palladium-nickel…) are
  valued metal by metal and summed, with an honest "coverage" figure showing how much of the
  basket the model actually prices.
- **A screener** that ranks the universe by revaluation upside, with visual signals for stale
  studies, tiny NPV bases, and data that deserves skepticism — because a confident wrong number
  is the most expensive output a financial platform can produce.

## The hard part

Extraction is easy to do badly. The pipeline is built around one principle: **a number is not
data until it has been earned, verified, and correctly computed.**

- Deterministic parsers handle the ASX's standardized forms; a language model handles the
  free-format documents — and everything it produces is checked by deterministic guards
  downstream (unit sanity, magnitude plausibility, cross-source reconciliation).
- When a value can't be trusted, the system says *unknown* rather than guessing. Missing data
  drops a "coverage" score instead of fabricating a number.
- Every computed figure carries its provenance and its warnings, all the way to the screen.

The result: ~24,000 documents processed across ~170 companies, feasibility studies from a dozen
commodities valued daily against live prices, and a screener where every number can explain
itself.

## Under the hood (briefly)

Python / Flask API · SQLite · Next.js + Tailwind frontend · Gemini for free-format document
extraction · deployed on Railway with a daily automated ingest-classify-revalue pipeline.
~440 automated tests anchor the parsers to real filings.

## Author

Built by **Omar Amine** — [omar.amine.pro@gmail.com](mailto:omar.amine.pro@gmail.com)
