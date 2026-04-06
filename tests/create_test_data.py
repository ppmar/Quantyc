"""
Create test data for pipeline validation.

Generates sample PDFs that mimic real ASX announcements, populates the
database, and allows the full pipeline to be tested end-to-end without
needing live ASX API access.

Usage:
    python -m tests.create_test_data
"""

import os
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_connection, init_db

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def _create_appendix_5b_pdf(ticker: str, doc_id: str) -> str:
    """Create a minimal Appendix 5B-like PDF using pdfplumber-compatible format."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    out_dir = RAW_DIR / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{doc_id}.pdf"

    doc = SimpleDocTemplate(str(path), pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Appendix 5B", styles["Title"]))
    elements.append(Paragraph("Mining exploration entity or oil and gas exploration entity quarterly cash flow report", styles["Normal"]))
    elements.append(Spacer(1, 20))

    # Cash flow table mimicking the ASX Appendix 5B format
    data = [
        ["", "Current quarter\n$A'000", "Year to date\n$A'000"],
        ["1. Cash flows from operating activities", "", ""],
        ["1.1 Receipts from customers", "150", "580"],
        ["1.2 Payments for exploration & evaluation", "(2,450)", "(9,200)"],
        ["1.9 Net cash from / (used in) operating activities", "(2,300)", "(8,620)"],
        ["", "", ""],
        ["2. Cash flows from investing activities", "", ""],
        ["2.1 Payments to acquire property, plant & equipment", "(180)", "(520)"],
        ["2.6 Net cash from / (used in) investing activities", "(180)", "(520)"],
        ["", "", ""],
        ["4. Cash and cash equivalents at end of quarter", "", ""],
        ["4.1 Cash and cash equivalents at end of quarter", "8,750", "8,750"],
    ]

    table = Table(data, colWidths=[300, 100, 100])
    table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    elements.append(table)

    doc.build(elements)
    return str(path)


def _create_resource_pdf(ticker: str, doc_id: str) -> str:
    """Create a minimal JORC resource announcement PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    out_dir = RAW_DIR / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{doc_id}.pdf"

    doc = SimpleDocTemplate(str(path), pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Mineral Resource Estimate Update", styles["Title"]))
    elements.append(Paragraph(f"{ticker} Gold Project — JORC 2012 Compliant", styles["Normal"]))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        "The Company is pleased to announce an updated Mineral Resource Estimate "
        "for its flagship gold project. The resource estimate was prepared in "
        "accordance with the JORC Code 2012 at a cut-off grade of 0.5 g/t Au.",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 15))

    # JORC resource table
    data = [
        ["Category", "Tonnes (Mt)", "Grade (g/t Au)", "Contained Gold (koz)"],
        ["Measured", "5.2", "2.1", "351"],
        ["Indicated", "12.8", "1.8", "741"],
        ["Inferred", "8.5", "1.4", "383"],
        ["Total", "26.5", "1.73", "1,475"],
    ]

    table = Table(data, colWidths=[100, 100, 100, 120])
    table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("Cut-off grade: 0.5 g/t Au. 100% attributable to the Company.", styles["Normal"]))

    doc.build(elements)
    return str(path)


def _create_study_pdf(ticker: str, doc_id: str) -> str:
    """Create a minimal PFS study announcement PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    out_dir = RAW_DIR / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{doc_id}.pdf"

    doc = SimpleDocTemplate(str(path), pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Pre-Feasibility Study Results", styles["Title"]))
    elements.append(Paragraph(f"{ticker} Gold Project — Robust Economics Confirmed", styles["Normal"]))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        "The Company announces the results of its Pre-Feasibility Study (PFS) "
        "for the Gold Project. The PFS confirms robust project economics with a "
        "post-tax NPV of US$285 million and an IRR of 32.5%.",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 15))

    # Study summary table
    data = [
        ["Parameter", "Value"],
        ["Study type", "Pre-Feasibility Study (PFS)"],
        ["Mine life", "12 years"],
        ["Annual production", "145 koz/yr"],
        ["Processing recovery", "93.5%"],
        ["Initial capital cost (capex)", "US$180 million"],
        ["Sustaining capital", "US$45 million"],
        ["All-in sustaining cost (AISC)", "US$1,050/oz"],
        ["Operating cost", "US$38/t processed"],
        ["Post-tax NPV (8% discount)", "US$285 million"],
        ["Post-tax IRR", "32.5%"],
        ["Gold price assumption", "US$1,850/oz"],
        ["AUD/USD exchange rate", "0.68"],
        ["Discount rate", "8%"],
    ]

    table = Table(data, colWidths=[200, 200])
    table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
    ]))
    elements.append(table)

    doc.build(elements)
    return str(path)


def _create_capital_raise_pdf(ticker: str, doc_id: str) -> str:
    """Create a minimal capital raise announcement PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet

    out_dir = RAW_DIR / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{doc_id}.pdf"

    doc = SimpleDocTemplate(str(path), pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Placement to Institutional Investors", styles["Title"]))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        f"{ticker} Limited (ASX: {ticker}) is pleased to announce it has received firm "
        "commitments from institutional and sophisticated investors to raise A$12.5 million "
        "through the issue of 125,000,000 new fully paid ordinary shares at $0.10 per share.",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        "Each participant will also receive one free-attaching option for every two shares "
        "subscribed, resulting in 62,500,000 options with an exercise price of $0.15 per "
        "option, expiring 30 June 2028.",
        styles["Normal"]
    ))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        "Funds will be used to advance the company's flagship gold project including "
        "resource definition drilling, feasibility study work, and general working capital.",
        styles["Normal"]
    ))

    doc.build(elements)
    return str(path)


def create_test_data():
    """Create the full set of test documents and database entries."""
    # Check for reportlab
    try:
        import reportlab
    except ImportError:
        print("Installing reportlab for PDF generation...")
        os.system(f"{sys.executable} -m pip install -q reportlab")
        import reportlab

    init_db()
    conn = get_connection()

    # Insert test company
    ticker = "TST"
    conn.execute(
        """INSERT OR REPLACE INTO companies
           (ticker, name, primary_commodity, reporting_currency, updated_at)
           VALUES (?, ?, ?, 'AUD', CURRENT_TIMESTAMP)""",
        (ticker, "Test Gold Mines Ltd", "gold"),
    )

    # Insert macro assumptions for valuation
    conn.execute(
        """INSERT OR REPLACE INTO macro_assumptions
           (date, gold_spot_usd, copper_spot_usd, lithium_spot_usd, silver_spot_usd,
            aud_usd, base_discount_rate, updated_at)
           VALUES ('2026-04-06', 3050.0, 4.25, 12500.0, 32.50, 0.63, 0.08, CURRENT_TIMESTAMP)"""
    )

    test_docs = [
        {
            "id": "test_5b_001",
            "doc_type": "appendix_5b",
            "header": "Appendix 5B - Quarterly Cash Flow Report Q1 2026",
            "date": "2026-03-31",
            "creator": _create_appendix_5b_pdf,
        },
        {
            "id": "test_res_001",
            "doc_type": "resource_update",
            "header": "Updated Mineral Resource Estimate - Gold Project",
            "date": "2026-02-15",
            "creator": _create_resource_pdf,
        },
        {
            "id": "test_study_001",
            "doc_type": "study",
            "header": "Pre-Feasibility Study Results - Gold Project",
            "date": "2026-01-20",
            "creator": _create_study_pdf,
        },
        {
            "id": "test_raise_001",
            "doc_type": "capital_raise",
            "header": "Placement to Raise $12.5M",
            "date": "2025-12-10",
            "creator": _create_capital_raise_pdf,
        },
    ]

    for td in test_docs:
        print(f"Creating {td['doc_type']}: {td['header']}")
        local_path = td["creator"](ticker, td["id"])

        conn.execute(
            """INSERT OR REPLACE INTO documents
               (id, company_ticker, doc_type, header, announcement_date,
                url, local_path, parse_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
            (
                td["id"], ticker, td["doc_type"], td["header"], td["date"],
                f"https://test.example.com/{td['id']}.pdf", local_path,
            ),
        )

    conn.commit()
    conn.close()
    print(f"\nCreated {len(test_docs)} test documents for ticker {ticker}")
    print("Database and PDFs ready for pipeline testing.")


if __name__ == "__main__":
    create_test_data()
