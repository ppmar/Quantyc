#!/usr/bin/env python3
"""
Merge project forks: rows of the same company whose names normalize to the
same canonical name ("Paris" / "Paris Silver", "Rebecca-Roe" / "Rebecca-Roe
Gold"). Forks let one study land twice with contradictory numbers and defeat
both the study dedup and the latest-study selection.

Keeper = lowest project_id in the group. Children repointed: studies,
revaluations, project_commodities (deduped), resources,
project_stage_inferences. A repointed study that collides with the keeper's
unique (project_id, study_stage, post_tax_npv) index is itself a duplicate —
it and its revaluations are deleted. The keeper's display name is set to the
canonical form.

Usage:
    python scripts/merge_project_forks.py [--dry-run]
"""
import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)


def merge_forks(conn, dry_run: bool) -> dict:
    from pipeline.orchestrator import normalize_project_name

    groups: dict[tuple[int, str], list] = defaultdict(list)
    for row in conn.execute(
        """SELECT p.project_id, p.company_id, p.project_name, c.name AS company_name
           FROM projects p JOIN companies c USING(company_id) ORDER BY p.project_id"""
    ).fetchall():
        key = (
            row["company_id"],
            normalize_project_name(row["project_name"], row["company_name"]).lower(),
        )
        groups[key].append(row)

    stats = {"groups_merged": 0, "projects_removed": 0, "studies_repointed": 0,
             "dup_studies_deleted": 0, "revals_deleted": 0}

    for (company_id, norm), rows in groups.items():
        if len(rows) < 2:
            continue
        keeper = rows[0]
        forks = rows[1:]
        canonical = normalize_project_name(keeper["project_name"], keeper["company_name"])
        stats["groups_merged"] += 1
        logger.info(
            "Merging company %d %r: keeper #%d (%r), forks %s",
            company_id, canonical, keeper["project_id"], keeper["project_name"],
            [(r["project_id"], r["project_name"]) for r in forks],
        )
        if dry_run:
            stats["projects_removed"] += len(forks)
            continue

        for fork in forks:
            fid = fork["project_id"]
            kid = keeper["project_id"]

            # Studies: repoint unless it collides with the keeper's dedup index
            # (same stage + npv, NULL-aware) — then it IS a duplicate: drop it
            # and its revaluations.
            for s in conn.execute(
                "SELECT study_id, study_stage, post_tax_npv FROM studies WHERE project_id = ?",
                (fid,),
            ).fetchall():
                clash = conn.execute(
                    """SELECT study_id FROM studies
                       WHERE project_id = ? AND study_stage = ? AND post_tax_npv IS ?""",
                    (kid, s["study_stage"], s["post_tax_npv"]),
                ).fetchone()
                if clash:
                    n_rev = conn.execute(
                        "DELETE FROM revaluations WHERE study_id = ?", (s["study_id"],)
                    ).rowcount
                    conn.execute("DELETE FROM studies WHERE study_id = ?", (s["study_id"],))
                    stats["dup_studies_deleted"] += 1
                    stats["revals_deleted"] += n_rev
                    logger.info("  dup study #%d (clashes with keeper #%d) deleted (+%d revals)",
                                s["study_id"], clash["study_id"], n_rev)
                else:
                    conn.execute(
                        "UPDATE studies SET project_id = ? WHERE study_id = ?",
                        (kid, s["study_id"]),
                    )
                    stats["studies_repointed"] += 1

            conn.execute("UPDATE revaluations SET project_id = ? WHERE project_id = ?", (kid, fid))
            conn.execute("UPDATE resources SET project_id = ? WHERE project_id = ?", (kid, fid))
            conn.execute(
                "UPDATE project_stage_inferences SET project_id = ? WHERE project_id = ?",
                (kid, fid),
            )

            # Commodities: move only what the keeper doesn't already have.
            for pc in conn.execute(
                "SELECT id, commodity, is_primary FROM project_commodities WHERE project_id = ?",
                (fid,),
            ).fetchall():
                exists = conn.execute(
                    "SELECT 1 FROM project_commodities WHERE project_id = ? AND commodity = ?",
                    (kid, pc["commodity"]),
                ).fetchone()
                if exists:
                    conn.execute("DELETE FROM project_commodities WHERE id = ?", (pc["id"],))
                else:
                    conn.execute(
                        "UPDATE project_commodities SET project_id = ? WHERE id = ?",
                        (kid, pc["id"]),
                    )

            conn.execute("DELETE FROM projects WHERE project_id = ?", (fid,))
            stats["projects_removed"] += 1

        conn.execute(
            "UPDATE projects SET project_name = ? WHERE project_id = ?",
            (canonical, keeper["project_id"]),
        )

    if not dry_run:
        conn.commit()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from db import get_connection
    conn = get_connection()
    stats = merge_forks(conn, args.dry_run)
    conn.close()
    print(f"\nMerge forks: {stats}{' (dry run)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
