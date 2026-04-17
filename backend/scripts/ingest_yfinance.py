#!/usr/bin/env python3
"""
VERDIQ — Strategy 2: yfinance Free Batch Ingestion Script
==========================================================
100% free cron-ready script. Runs every weekend to update your entire
database using the yfinance normalizer (no API credits consumed).

Covers 3-4 years of data per stock (Yahoo Finance's Indian stock limit).
Use Strategy 1 (Apify) for deeper 10-year histories on your top stocks.

Usage:
    # Single ticker test
    python -m backend.scripts.ingest_yfinance --ticker TATAMOTORS

    # Full NIFTY 50 (takes ~2-3 min, all free)
    python -m backend.scripts.ingest_yfinance --list nifty50

    # Concurrently (faster — runs 5 tickers at once)
    python -m backend.scripts.ingest_yfinance --list nifty50 --workers 5

    # Custom ticker file
    python -m backend.scripts.ingest_yfinance --file my_tickers.txt

    # Dry run
    python -m backend.scripts.ingest_yfinance --ticker INFY --dry-run

Cost: Completely free. Yahoo Finance has no rate limits for small batches.
      For 500+ stocks, add --workers 3 and a small sleep to be polite.
"""

import os
import sys
import time
import argparse
import concurrent.futures
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

from backend.adapters.yfinance_normalizer import YFinanceNormalizer
from backend.adapters.supabase_adapter import SupabaseAdapter
from backend.scripts.ingest_apify import NIFTY_50, NIFTY_NEXT_50  # reuse the lists


def process_ticker(ticker: str, dry_run: bool) -> tuple[str, str, str]:
    """
    Process a single ticker. Returns (ticker, status, message).
    Designed to be called in a thread pool for concurrent ingestion.
    """
    try:
        fundamentals = YFinanceNormalizer.fetch_and_normalize(ticker)

        if not fundamentals.history_years:
            return ticker, "failed", "yfinance returned no data (delisted or wrong symbol?)"

        summary = (
            f"P/E: {fundamentals.current_pe:.1f}x | "
            f"ROE: {fundamentals.roe_percentage:.1f}% | "
            f"D/E: {fundamentals.debt_to_equity:.2f}x | "
            f"Rev Growth: {fundamentals.revenue_growth_3yr:.1f}% | "
            f"Years: {len(fundamentals.history_years)}"
        )

        if dry_run:
            return ticker, "skipped", summary

        ok = SupabaseAdapter.write_historical_financials(fundamentals)
        if ok:
            return ticker, "success", summary
        else:
            return ticker, "failed", "Supabase write returned False"

    except Exception as e:
        return ticker, "failed", str(e)


def run_batch(tickers: list[str], dry_run: bool = False, workers: int = 1) -> dict:
    """
    Processes a list of tickers through the yfinance pipeline.
    Supports concurrent execution via a thread pool.
    """
    results = {"success": [], "failed": [], "skipped": []}
    total = len(tickers)

    print(f"\n{'='*60}")
    print(f"  VERDIQ yfinance Ingestion — {total} tickers | workers={workers}")
    print(f"  Dry run: {dry_run}")
    print(f"{'='*60}\n")

    if workers == 1:
        # Sequential — safer for rate limits
        for i, ticker in enumerate(tickers, 1):
            print(f"[{i:>3}/{total}] {ticker}...", end=" ", flush=True)
            t, status, msg = process_ticker(ticker, dry_run)
            icon = "✅" if status == "success" else ("⏭" if status == "skipped" else "❌")
            print(f"{icon} {msg}")
            results[status].append(t)
            time.sleep(0.5)  # gentle rate-limit

    else:
        # Concurrent — significantly faster for large batches
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_ticker = {
                pool.submit(process_ticker, ticker, dry_run): ticker
                for ticker in tickers
            }
            completed = 0
            for future in concurrent.futures.as_completed(future_to_ticker):
                completed += 1
                t, status, msg = future.result()
                icon = "✅" if status == "success" else ("⏭" if status == "skipped" else "❌")
                print(f"[{completed:>3}/{total}] {t:20s} {icon} {msg}")
                results[status].append(t)

    print(f"\n{'='*60}")
    print(f"  Ingestion complete.")
    print(f"  ✅ Success : {len(results['success'])}")
    print(f"  ❌ Failed  : {len(results['failed'])}")
    print(f"  ⏭  Skipped : {len(results['skipped'])}")
    if results["failed"]:
        print(f"\n  Failed tickers (check symbols or delist status):")
        for t in results["failed"]:
            print(f"    - {t}")
    print(f"{'='*60}\n")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="VERDIQ yfinance Batch Ingestion — 100% free weekly data refresh."
    )
    parser.add_argument("--ticker", type=str, help="Single ticker (e.g. TATAMOTORS)")
    parser.add_argument(
        "--list",
        choices=["nifty50", "nifty100"],
        help="Ingest a preset list"
    )
    parser.add_argument("--file", type=str, help="Path to file with one ticker per line")
    parser.add_argument("--dry-run", action="store_true", help="Normalize but skip Supabase write")
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of concurrent threads (default: 1 for safety)"
    )

    args = parser.parse_args()

    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.list == "nifty50":
        tickers = NIFTY_50
    elif args.list == "nifty100":
        tickers = NIFTY_50 + NIFTY_NEXT_50
    elif args.file:
        tickers = [
            line.strip().upper()
            for line in open(args.file)
            if line.strip() and not line.startswith("#")
        ]
    else:
        print("Error: specify --ticker, --list, or --file. Use --help for usage.")
        sys.exit(1)

    run_batch(tickers, dry_run=args.dry_run, workers=args.workers)


if __name__ == "__main__":
    main()
