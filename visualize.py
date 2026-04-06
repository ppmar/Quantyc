"""
Data Visualization Dashboard

Generates charts for drill results, resources, and pipeline overview.

Usage:
    python visualize.py
"""

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from db import get_connection

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(exist_ok=True)


def plot_drill_cross_section(conn, doc_id: str = None):
    """
    Cross-section view of drill holes — depth vs grade, coloured by Au g/t.
    Each hole is a vertical strip; intercepts are coloured blocks.
    """
    where = "WHERE source_doc_id = ?" if doc_id else ""
    params = (doc_id,) if doc_id else ()

    rows = conn.execute(f"""
        SELECT hole_id, from_m, to_m, interval_m, au_gt, au_eq_gt, sb_pct, is_including
        FROM drill_results {where}
        ORDER BY hole_id, from_m
    """, params).fetchall()

    if not rows:
        print("No drill results to plot")
        return

    # Group by hole
    holes = {}
    for r in rows:
        hid = r["hole_id"]
        if hid not in holes:
            holes[hid] = []
        holes[hid].append(dict(r))

    hole_ids = sorted(holes.keys())
    n_holes = len(hole_ids)

    fig, axes = plt.subplots(1, n_holes, figsize=(4 * n_holes, 12), sharey=True)
    if n_holes == 1:
        axes = [axes]

    # Colour scale based on AuEq
    max_grade = max(
        (r["au_eq_gt"] or r["au_gt"] or 0) for rows_list in holes.values() for r in rows_list
    )
    cmap = plt.cm.YlOrRd

    for ax, hid in zip(axes, hole_ids):
        intercepts = holes[hid]

        for intc in intercepts:
            from_m = intc["from_m"]
            to_m = intc["to_m"]
            if from_m is None or to_m is None:
                continue

            grade = intc["au_eq_gt"] or intc["au_gt"] or 0
            # Log-scale colour for better contrast
            norm_grade = np.log1p(grade) / np.log1p(max_grade) if max_grade > 0 else 0
            colour = cmap(min(norm_grade, 1.0))

            width = 0.8 if not intc["is_including"] else 0.5
            x_offset = 0 if not intc["is_including"] else 0.15

            rect = plt.Rectangle(
                (x_offset, from_m), width, to_m - from_m,
                facecolor=colour, edgecolor="grey", linewidth=0.3,
            )
            ax.add_patch(rect)

            # Label significant intercepts (>10 g/t AuEq)
            if grade > 10 and not intc["is_including"]:
                interval = intc["interval_m"] or (to_m - from_m)
                label = f"{interval:.1f}m\n@ {grade:.1f}"
                ax.text(
                    1.0, (from_m + to_m) / 2, label,
                    fontsize=6, va="center", ha="left", color="black",
                )

        # Formatting
        ax.set_xlim(-0.1, 2.0)
        min_depth = min((i["from_m"] or 0) for i in intercepts)
        max_depth = max((i["to_m"] or 0) for i in intercepts)
        ax.set_ylim(max_depth + 10, max(min_depth - 10, 0))
        ax.set_title(hid, fontsize=10, fontweight="bold")
        ax.set_xlabel("")
        ax.set_xticks([])

    axes[0].set_ylabel("Depth (m)", fontsize=11)

    # Colourbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(0, max_grade))
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=axes, shrink=0.6, pad=0.02)
    cbar.set_label("AuEq g/t", fontsize=10)

    fig.suptitle("Drill Hole Cross-Section — Grade by Depth", fontsize=14, fontweight="bold", y=0.98)
    fig.tight_layout(rect=[0, 0, 0.92, 0.96])

    path = OUT_DIR / "drill_cross_section.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_drill_top_intercepts(conn, doc_id: str = None, top_n: int = 20):
    """
    Horizontal bar chart of top intercepts by gram-metres (grade × width).
    """
    where = "WHERE source_doc_id = ?" if doc_id else ""
    params = (doc_id,) if doc_id else ()

    rows = conn.execute(f"""
        SELECT hole_id, from_m, interval_m, au_gt, au_eq_gt, sb_pct, is_including
        FROM drill_results {where}
        AND interval_m IS NOT NULL AND is_including = 0
        AND (au_eq_gt IS NOT NULL OR au_gt IS NOT NULL)
        ORDER BY COALESCE(au_eq_gt, au_gt) * interval_m DESC
        LIMIT ?
    """, (*params, top_n)).fetchall()

    if not rows:
        return

    labels = []
    gram_metres = []
    colours = []

    for r in rows:
        grade = r["au_eq_gt"] or r["au_gt"] or 0
        interval = r["interval_m"] or 0
        gm = grade * interval
        gram_metres.append(gm)

        from_m = r["from_m"] or 0
        labels.append(f"{r['hole_id']} from {from_m:.0f}m ({interval:.1f}m @ {grade:.1f} g/t)")

        # Colour by whether it has antimony
        colours.append("#c0392b" if r["sb_pct"] and r["sb_pct"] > 1 else "#f39c12")

    fig, ax = plt.subplots(figsize=(12, 8))
    y_pos = range(len(labels) - 1, -1, -1)
    ax.barh(y_pos, gram_metres, color=colours, edgecolor="white", height=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Gram-metres (g/t × m)", fontsize=11)
    ax.set_title(f"Top {top_n} Drill Intercepts by Gram-Metres", fontsize=14, fontweight="bold")

    # Legend
    au_patch = mpatches.Patch(color="#f39c12", label="Gold dominant")
    sb_patch = mpatches.Patch(color="#c0392b", label="Significant Sb (>1%)")
    ax.legend(handles=[au_patch, sb_patch], loc="lower right")

    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()

    path = OUT_DIR / "drill_top_intercepts.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_drill_plan_view(conn, doc_id: str = None):
    """
    Plan view (map) of drill hole collars coloured by best intercept grade.
    """
    where = "WHERE source_doc_id = ?" if doc_id else ""
    params = (doc_id,) if doc_id else ()

    rows = conn.execute(f"""
        SELECT hole_id, easting, northing, MAX(COALESCE(au_eq_gt, au_gt, 0)) as best_grade
        FROM drill_results {where}
        AND easting IS NOT NULL AND northing IS NOT NULL
        GROUP BY hole_id
    """, params).fetchall()

    if not rows:
        return

    fig, ax = plt.subplots(figsize=(10, 10))

    eastings = [r["easting"] for r in rows]
    northings = [r["northing"] for r in rows]
    grades = [r["best_grade"] for r in rows]
    hole_ids = [r["hole_id"] for r in rows]

    scatter = ax.scatter(
        eastings, northings,
        c=grades, cmap="YlOrRd", s=120, edgecolor="black", linewidth=0.5,
        vmin=0, vmax=max(grades),
    )

    for hid, e, n in zip(hole_ids, eastings, northings):
        ax.annotate(hid, (e, n), fontsize=7, ha="left", va="bottom",
                    xytext=(5, 5), textcoords="offset points")

    cbar = fig.colorbar(scatter, ax=ax, shrink=0.7)
    cbar.set_label("Best AuEq g/t", fontsize=10)

    ax.set_xlabel("Easting (GDA94)", fontsize=11)
    ax.set_ylabel("Northing (GDA94)", fontsize=11)
    ax.set_title("Drill Collar Plan View — Best Grade per Hole", fontsize=14, fontweight="bold")
    ax.set_aspect("equal")
    ax.grid(alpha=0.3)

    fig.tight_layout()
    path = OUT_DIR / "drill_plan_view.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_grade_histogram(conn, doc_id: str = None):
    """Histogram of Au g/t distribution across all intercepts."""
    where = "WHERE source_doc_id = ?" if doc_id else ""
    params = (doc_id,) if doc_id else ()

    rows = conn.execute(f"""
        SELECT au_gt FROM drill_results {where}
        AND au_gt IS NOT NULL AND au_gt > 0
    """, params).fetchall()

    if not rows:
        return

    grades = [r["au_gt"] for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Linear histogram
    ax1.hist(grades, bins=50, color="#2c3e50", edgecolor="white", alpha=0.8)
    ax1.set_xlabel("Au g/t", fontsize=11)
    ax1.set_ylabel("Count", fontsize=11)
    ax1.set_title("Grade Distribution (Linear)", fontsize=12)
    ax1.axvline(np.median(grades), color="red", linestyle="--", label=f"Median: {np.median(grades):.1f}")
    ax1.axvline(np.mean(grades), color="orange", linestyle="--", label=f"Mean: {np.mean(grades):.1f}")
    ax1.legend()

    # Log histogram (better for lognormal grade distributions)
    log_grades = [np.log10(g) for g in grades if g > 0]
    ax2.hist(log_grades, bins=40, color="#8e44ad", edgecolor="white", alpha=0.8)
    ax2.set_xlabel("log₁₀(Au g/t)", fontsize=11)
    ax2.set_ylabel("Count", fontsize=11)
    ax2.set_title("Grade Distribution (Log Scale)", fontsize=12)

    fig.suptitle("Gold Grade Distribution — All Intercepts", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])

    path = OUT_DIR / "grade_histogram.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_resource_breakdown(conn):
    """Stacked bar chart of resource categories by commodity."""
    rows = conn.execute("""
        SELECT p.ticker, r.category, r.contained_metal, r.contained_unit, r.commodity
        FROM resources r
        JOIN projects p ON r.project_id = p.id
        WHERE r.category != 'Total' AND r.contained_metal IS NOT NULL
        ORDER BY p.ticker
    """).fetchall()

    if not rows:
        print("No resource data to plot")
        return

    # Group by ticker
    tickers = {}
    for r in rows:
        t = r["ticker"]
        if t not in tickers:
            tickers[t] = {"Measured": 0, "Indicated": 0, "Inferred": 0}
        cat = r["category"]
        if cat in tickers[t]:
            tickers[t][cat] += r["contained_metal"]

    fig, ax = plt.subplots(figsize=(max(8, len(tickers) * 2), 6))

    ticker_names = list(tickers.keys())
    x = np.arange(len(ticker_names))
    width = 0.6

    measured = [tickers[t]["Measured"] for t in ticker_names]
    indicated = [tickers[t]["Indicated"] for t in ticker_names]
    inferred = [tickers[t]["Inferred"] for t in ticker_names]

    ax.bar(x, measured, width, label="Measured", color="#27ae60")
    ax.bar(x, indicated, width, bottom=measured, label="Indicated", color="#2980b9")
    ax.bar(x, inferred, width,
           bottom=[m + i for m, i in zip(measured, indicated)],
           label="Inferred", color="#e67e22")

    ax.set_xticks(x)
    ax.set_xticklabels(ticker_names, fontsize=11)
    unit = rows[0]["contained_unit"] if rows else "koz"
    ax.set_ylabel(f"Contained Metal ({unit})", fontsize=11)
    ax.set_title("Resource Breakdown by JORC Category", fontsize=14, fontweight="bold")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    path = OUT_DIR / "resource_breakdown.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_pipeline_dashboard(conn):
    """Overview dashboard showing document counts, parse status, and data coverage."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # 1. Documents by type
    ax = axes[0, 0]
    rows = conn.execute(
        "SELECT doc_type, COUNT(*) as n FROM documents GROUP BY doc_type ORDER BY n DESC"
    ).fetchall()
    if rows:
        types = [r["doc_type"] for r in rows]
        counts = [r["n"] for r in rows]
        colours = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6", "#1abc9c", "#95a5a6"]
        ax.barh(types, counts, color=colours[:len(types)], edgecolor="white")
        ax.set_xlabel("Count")
        ax.set_title("Documents by Type", fontweight="bold")
        for i, (t, c) in enumerate(zip(types, counts)):
            ax.text(c + 0.1, i, str(c), va="center", fontsize=10, fontweight="bold")

    # 2. Parse status
    ax = axes[0, 1]
    rows = conn.execute(
        "SELECT parse_status, COUNT(*) as n FROM documents GROUP BY parse_status"
    ).fetchall()
    if rows:
        statuses = [r["parse_status"] for r in rows]
        counts = [r["n"] for r in rows]
        status_colours = {"done": "#2ecc71", "pending": "#f39c12", "failed": "#e74c3c", "needs_review": "#3498db"}
        colours = [status_colours.get(s, "#95a5a6") for s in statuses]
        wedges, texts, autotexts = ax.pie(
            counts, labels=statuses, colors=colours, autopct="%1.0f%%",
            startangle=90, textprops={"fontsize": 10},
        )
        ax.set_title("Parse Status", fontweight="bold")

    # 3. Data coverage table
    ax = axes[1, 0]
    ax.axis("off")

    table_data = []

    # Companies
    n_companies = conn.execute("SELECT COUNT(*) as n FROM companies").fetchone()["n"]
    table_data.append(["Companies", str(n_companies)])

    # Documents
    n_docs = conn.execute("SELECT COUNT(*) as n FROM documents").fetchone()["n"]
    table_data.append(["Documents", str(n_docs)])

    # Drill intercepts
    n_drill = conn.execute("SELECT COUNT(*) as n FROM drill_results").fetchone()["n"]
    n_holes = conn.execute("SELECT COUNT(DISTINCT hole_id) as n FROM drill_results").fetchone()["n"]
    table_data.append(["Drill Intercepts", f"{n_drill} ({n_holes} holes)"])

    # Resources
    n_res = conn.execute("SELECT COUNT(*) as n FROM resources WHERE category != 'Total'").fetchone()["n"]
    table_data.append(["Resource Rows", str(n_res)])

    # Financials
    n_fin = conn.execute("SELECT COUNT(*) as n FROM company_financials").fetchone()["n"]
    table_data.append(["Financial Records", str(n_fin)])

    # Staging
    n_stage = conn.execute("SELECT COUNT(*) as n FROM staging_extractions").fetchone()["n"]
    table_data.append(["Staging Extractions", str(n_stage)])

    # Review items
    n_review = conn.execute("SELECT COUNT(*) as n FROM staging_extractions WHERE needs_review = 1").fetchone()["n"]
    table_data.append(["Needs Review", str(n_review)])

    table = ax.table(
        cellText=table_data,
        colLabels=["Metric", "Value"],
        cellLoc="left",
        loc="center",
        colWidths=[0.5, 0.4],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.8)
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor("#2c3e50")
            cell.set_text_props(color="white", fontweight="bold")
        elif row % 2 == 0:
            cell.set_facecolor("#ecf0f1")
    ax.set_title("Data Coverage Summary", fontweight="bold", pad=20)

    # 4. Cash runway (if available)
    ax = axes[1, 1]
    rows = conn.execute("""
        SELECT ticker, cash_aud, quarterly_burn, cash_runway_months
        FROM company_financials
        WHERE cash_aud IS NOT NULL AND cash_runway_months IS NOT NULL
        ORDER BY cash_runway_months ASC
    """).fetchall()

    if rows:
        tickers = [r["ticker"] for r in rows]
        runways = [r["cash_runway_months"] for r in rows]
        colours = ["#e74c3c" if r < 6 else "#f39c12" if r < 12 else "#2ecc71" for r in runways]

        bars = ax.barh(tickers, runways, color=colours, edgecolor="white")
        ax.axvline(6, color="red", linestyle="--", alpha=0.7, label="6-month warning")
        ax.axvline(12, color="orange", linestyle="--", alpha=0.5, label="12-month marker")
        ax.set_xlabel("Months")
        ax.set_title("Cash Runway", fontweight="bold")
        ax.legend(fontsize=8)
        ax.grid(axis="x", alpha=0.3)

        for bar, val in zip(bars, runways):
            ax.text(val + 0.2, bar.get_y() + bar.get_height() / 2,
                    f"{val:.1f}mo", va="center", fontsize=9)
    else:
        ax.text(0.5, 0.5, "No financial data yet", ha="center", va="center",
                transform=ax.transAxes, fontsize=12, color="grey")
        ax.set_title("Cash Runway", fontweight="bold")

    fig.suptitle("ASX Junior Miner Pipeline — Dashboard", fontsize=16, fontweight="bold", y=1.01)
    fig.tight_layout()

    path = OUT_DIR / "pipeline_dashboard.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def main():
    conn = get_connection()

    # Find the SXG doc_id for drill-specific plots
    sxg_doc = conn.execute(
        "SELECT id FROM documents WHERE company_ticker = 'SXG' AND doc_type = 'drill_results' LIMIT 1"
    ).fetchone()
    drill_doc_id = sxg_doc["id"] if sxg_doc else None

    print("Generating visualizations...\n")

    plot_drill_cross_section(conn, drill_doc_id)
    plot_drill_top_intercepts(conn, drill_doc_id)
    plot_drill_plan_view(conn, drill_doc_id)
    plot_grade_histogram(conn, drill_doc_id)
    plot_resource_breakdown(conn)
    plot_pipeline_dashboard(conn)

    conn.close()
    print(f"\nAll charts saved to: {OUT_DIR}/")


if __name__ == "__main__":
    main()
