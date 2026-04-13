#!/usr/bin/env python3
"""
CLI: Poll ASX for a list of tickers.

Usage:
    python scripts/run_poller.py --tickers DEG,WAF --count 20
    python scripts/run_poller.py --file pilot_tickers.txt
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import init_db
from ingest.asx_poller import poll_tickers


def main():
    parser = argparse.ArgumentParser(description="Poll ASX announcements")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tickers", type=str, help="Comma-separated tickers")
    group.add_argument("--file", type=str, help="File with one ticker per line")
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        with open(args.file) as f:
            tickers = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    stats = poll_tickers(tickers, count=args.count)
    print(f"\nDone: {stats}")


if __name__ == "__main__":
    main()
