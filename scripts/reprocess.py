#!/usr/bin/env python3
"""
CLI: Re-run the orchestrator on all pending/classified documents.

Usage:
    python scripts/reprocess.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import init_db
from pipeline.orchestrator import run_orchestrator


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    stats = run_orchestrator()
    print(f"\nOrchestrator results: {stats}")


if __name__ == "__main__":
    main()
