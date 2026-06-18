-- Production detector: Appendix 5B line 1.1 "Receipts from customers".
-- A producer books material customer receipts; an explorer/developer ~0. The
-- net operating figure (1.9) we already store goes negative during ramp-up, so
-- it can't prove production — 1.1 (gross sales inflow) can. Feeds the
-- deterministic production floor in pipeline/stage_floor.py.
ALTER TABLE company_financials ADD COLUMN receipts_from_customers REAL;
