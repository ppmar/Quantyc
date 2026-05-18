#!/usr/bin/env python3
"""
Export DFS study + revaluation data as a self-contained SQL script
that can be imported into a Railway DB with different auto-increment IDs.

Uses ticker as the stable key, not numeric IDs.
"""
import json
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

conn = sqlite3.connect('db/quantyc.db')
conn.row_factory = sqlite3.Row


def q(v):
    """Quote a value for SQL."""
    if v is None:
        return 'NULL'
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


lines = []
lines.append("-- DFS study + revaluation sync script")
lines.append("-- Generated from local extraction, resolves IDs by ticker/project_name")
lines.append("")

# Get all studies with their context
studies = conn.execute("""
    SELECT s.*, p.project_name, c.ticker,
           pc.commodity as primary_commodity
    FROM studies s
    JOIN projects p ON s.project_id = p.project_id
    JOIN companies c ON p.company_id = c.company_id
    LEFT JOIN project_commodities pc ON pc.project_id = p.project_id AND pc.is_primary = 1
""").fetchall()

# Get revaluations
revals = conn.execute("""
    SELECT r.*, p.project_name, c.ticker
    FROM revaluations r
    JOIN projects p ON r.project_id = p.project_id
    JOIN companies c ON r.company_id = c.company_id
""").fetchall()

# Get commodity prices
prices = conn.execute("SELECT * FROM commodity_prices").fetchall()

# Ensure companies exist
tickers = set()
for s in studies:
    tickers.add(s['ticker'])

for ticker in sorted(tickers):
    lines.append(f"INSERT OR IGNORE INTO companies (ticker, first_seen_at, last_updated_at) "
                 f"VALUES ('{ticker}', datetime('now'), datetime('now'));")

lines.append("")

# Ensure documents exist (referenced by studies)
doc_ids = set(s['document_id'] for s in studies)
docs = conn.execute(f"SELECT * FROM documents WHERE document_id IN ({','.join(str(d) for d in doc_ids)})").fetchall()

for doc in docs:
    lines.append(
        f"INSERT OR IGNORE INTO documents (ticker, url, sha256, source, announcement_date, "
        f"ingested_at, doc_type, header, parse_status, local_path) "
        f"VALUES ({q(doc['ticker'])}, {q(doc['url'])}, {q(doc['sha256'])}, {q(doc['source'])}, "
        f"{q(doc['announcement_date'])}, {q(doc['ingested_at'])}, {q(doc['doc_type'])}, "
        f"{q(doc['header'])}, 'parsed', '');"
    )

lines.append("")

# Create projects (by ticker + project_name)
for s in studies:
    lines.append(
        f"INSERT OR IGNORE INTO projects (company_id, project_name, created_at) "
        f"SELECT company_id, {q(s['project_name'])}, datetime('now') "
        f"FROM companies WHERE ticker = {q(s['ticker'])};"
    )

lines.append("")

# Project commodities
for s in studies:
    if s['primary_commodity']:
        lines.append(
            f"INSERT OR IGNORE INTO project_commodities (project_id, commodity, is_primary) "
            f"SELECT p.project_id, {q(s['primary_commodity'])}, 1 "
            f"FROM projects p JOIN companies c ON p.company_id = c.company_id "
            f"WHERE c.ticker = {q(s['ticker'])} AND p.project_name = {q(s['project_name'])};"
        )

lines.append("")

# Commodity prices
for p in prices:
    lines.append(
        f"INSERT OR IGNORE INTO commodity_prices (commodity, price_usd, unit, source, fetched_at) "
        f"VALUES ({q(p['commodity'])}, {p['price_usd']}, {q(p['unit'])}, {q(p['source'])}, {q(p['fetched_at'])});"
    )

lines.append("")

# Studies — resolve project_id and document_id dynamically
for s in studies:
    lines.append(f"""INSERT OR IGNORE INTO studies (
    project_id, document_id, study_stage, study_date, mine_life_years,
    annual_production, recovery_pct, initial_capex, sustaining_capex, opex,
    post_tax_npv, irr_pct, assumed_price_deck, assumed_fx, reporting_currency,
    discount_rate_pct, pre_tax_npv, aisc_per_unit, aisc_unit, payback_years,
    extraction_method, extraction_model, tax_rate_pct
) SELECT
    p.project_id,
    (SELECT document_id FROM documents WHERE sha256 = {q(conn.execute('SELECT sha256 FROM documents WHERE document_id = ?', (s['document_id'],)).fetchone()['sha256'])} LIMIT 1),
    {q(s['study_stage'])}, {q(s['study_date'])}, {q(s['mine_life_years'])},
    {q(s['annual_production'])}, {q(s['recovery_pct'])}, {q(s['initial_capex'])}, {q(s['sustaining_capex'])}, {q(s['opex'])},
    {q(s['post_tax_npv'])}, {q(s['irr_pct'])}, {q(s['assumed_price_deck'])}, {q(s['assumed_fx'])}, {q(s['reporting_currency'])},
    {q(s['discount_rate_pct'])}, {q(s['pre_tax_npv'])}, {q(s['aisc_per_unit'])}, {q(s['aisc_unit'])}, {q(s['payback_years'])},
    {q(s['extraction_method'])}, {q(s['extraction_model'])}, {q(s['tax_rate_pct'])}
FROM projects p
JOIN companies c ON p.company_id = c.company_id
WHERE c.ticker = {q(s['ticker'])} AND p.project_name = {q(s['project_name'])}
AND NOT EXISTS (
    SELECT 1 FROM studies s2 WHERE s2.project_id = p.project_id
    AND s2.study_stage = {q(s['study_stage'])} AND s2.post_tax_npv = {q(s['post_tax_npv'])}
);""")

lines.append("")

# Revaluations — resolve by study match
for r in revals:
    lines.append(f"""INSERT OR IGNORE INTO revaluations (
    study_id, project_id, company_id, computed_at, commodity,
    price_dfs, price_spot, price_spot_id, fx_rate, fx_rate_price_id,
    annual_production, annual_production_unit, mine_life_years,
    discount_rate_pct, tax_rate_pct, annuity_factor,
    npv_dfs, npv_spot, npv_uplift, npv_uplift_pct,
    method_version, warnings
) SELECT
    s.study_id, p.project_id, c.company_id, {q(r['computed_at'])}, {q(r['commodity'])},
    {q(r['price_dfs'])}, {q(r['price_spot'])},
    (SELECT price_id FROM commodity_prices WHERE commodity = {q(r['commodity'])} ORDER BY fetched_at DESC LIMIT 1),
    {q(r['fx_rate'])},
    CASE WHEN {q(r['fx_rate'])} IS NOT NULL THEN (SELECT price_id FROM commodity_prices WHERE commodity = 'AUDUSD' ORDER BY fetched_at DESC LIMIT 1) ELSE NULL END,
    {q(r['annual_production'])}, {q(r['annual_production_unit'])}, {q(r['mine_life_years'])},
    {q(r['discount_rate_pct'])}, {q(r['tax_rate_pct'])}, {q(r['annuity_factor'])},
    {q(r['npv_dfs'])}, {q(r['npv_spot'])}, {q(r['npv_uplift'])}, {q(r['npv_uplift_pct'])},
    {q(r['method_version'])}, {q(r['warnings'])}
FROM studies s
JOIN projects p ON s.project_id = p.project_id
JOIN companies c ON p.company_id = c.company_id
WHERE c.ticker = {q(r['ticker'])} AND p.project_name = {q(r['project_name'])}
AND s.post_tax_npv = {q(conn.execute('SELECT post_tax_npv FROM studies WHERE study_id = ?', (r['study_id'],)).fetchone()['post_tax_npv'])}
AND NOT EXISTS (
    SELECT 1 FROM revaluations r2 WHERE r2.study_id = s.study_id
);""")

sql = "\n".join(lines)
with open('/tmp/railway_sync.sql', 'w') as f:
    f.write(sql)

print(f"Generated {len(lines)} lines")
print(f"Tickers: {sorted(tickers)}")
print(f"Studies: {len(studies)}")
print(f"Revaluations: {len(revals)}")
