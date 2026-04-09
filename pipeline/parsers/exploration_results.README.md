# exploration_results parser

Deterministic, LLM-free parser for ASX/TSX exploration results announcements from junior gold/polymetallic miners.

## Public API

```python
from pipeline.parsers.exploration_results import parse

payload = parse(
    pdf_path="path/to/announcement.pdf",
    ticker="SX2",
    doc_id="abc123",
)
```

Returns an `ExplorationResultsPayload` (Pydantic model) with:
- `release_date` -- date from PDF header
- `headline_intercept` -- best result from page 1
- `project_totals` -- cumulative drilling statistics
- `metal_equivalent` -- AuEq formula and price assumptions
- `drill_collars` -- hole locations and depths
- `composite_intersections` -- composited assay intervals with Including sub-rows
- `individual_assays` -- raw per-sample assay data
- `extraction_warnings` / `extraction_errors` -- quality signals

## Exceptions

- `WrongDocumentTypeError` -- PDF is not an exploration results document
- `PDFReadError` -- pdfplumber cannot open the file
- `EmptyPDFError` -- PDF has zero pages

## Known limitations

- No OCR support -- scanned PDFs will fail
- English-language ASX/TSX format only
- AuEq formula detection requires the standard `AuEq = Au (g/t) + X * Sb (%)` format
- Unicode math italic characters (common in PDF rendering) are handled
- Performance: ~8s on a 35-page PDF (under 15s hard ceiling)
- Validated on SXG (Southern Cross Gold) releases; other issuers may need regex tuning

## Routing

Documents classified as `exploration_results` by the classifier are dispatched here.
Falls back to `drill_results` for documents that don't match the exploration_results profile.
