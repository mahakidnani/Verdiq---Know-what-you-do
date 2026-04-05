#!/usr/bin/env python3
"""
VERDIQ — Strategy 1: Apify Batch Ingestion Script
==================================================
Cron-ready script that pulls 10-year Screener.in fundamentals for a list of
NSE tickers via Apify and writes ALL data into Supabase.

Run this once to pre-populate your top NIFTY stocks. Then schedule it
weekly (e.g. Sunday 2am) to keep the data fresh.

Usage:
    # Single ticker (test run)
    python -m backend.scripts.ingest_apify --ticker TATAMOTORS

    # Full NIFTY 50 batch
    python -m backend.scripts.ingest_apify --list nifty50

    # Custom list from file (one ticker per line)
    python -m backend.scripts.ingest_apify --file my_tickers.txt

    # Dry run (parse + normalize, skip Supabase write)
    python -m backend.scripts.ingest_apify --ticker INFY --dry-run

Cost: Each Apify call uses a small fraction of your free monthly credits.
      NIFTY 50 batch ≈ 50 calls ≈ ~$0.50 of free credits.

Pro-tip: Run once → save JSON output → build UI against dummy_data.json.
         Only reconnect live API when UI is ready.
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Load env from backend/.env
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

from backend.adapters.apify_screener import ApifyScreenerAdapter
from backend.adapters.supabase_adapter import SupabaseAdapter

# ── NIFTY 50 tickers (standard NSE symbols, no .NS suffix needed) ──
NIFTY_50 = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
    "HINDUNILVR", "SBIN", "BAJFINANCE", "BHARTIARTL", "ITC",
    "KOTAKBANK", "LT", "AXISBANK", "ASIANPAINT", "MARUTI",
    "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO", "ONGC",
    "NESTLEIND", "TATAMOTORS", "HCLTECH", "TECHM", "BAJAJFINSV",
    "POWERGRID", "NTPC", "TATASTEEL", "COALINDIA", "ADANIPORTS",
    "JSWSTEEL", "HINDALCO", "GRASIM", "CIPLA", "DRREDDY",
    "DIVISLAB", "APOLLOHOSP", "BPCL", "INDUSINDBK", "EICHERMOT",
    "HEROMOTOCO", "BRITANNIA", "SBILIFE", "HDFCLIFE", "BAJAJ-AUTO",
    "M&M", "TATACONSUM", "ADANIENT", "VEDL", "UPL",
]

NIFTY_NEXT_50 = [
    "AMBUJACEM", "AUROPHARMA", "BANDHANBNK", "BERGEPAINT", "BIOCON",
    "BOSCHLTD", "CANBK", "CHOLAFIN", "COLPAL", "DLF",
    "GAIL", "GODREJCP", "HAVELLS", "ICICIPRULI", "IDEA",
    "INDHOTEL", "INDUSTOWER", "IRCTC", "JINDALSTEL", "JUBLFOOD",
    "LUPIN", "MARICO", "MCDOWELL-N", "MOTHERSON", "MPHASIS",
    "PAGEIND", "PIIND", "PNB", "RECLTD", "SRF",
    "SIEMENS", "TORNTPHARM", "TRENT", "TVSMOTOR", "UBL",
    "UNITED SPIRITS", "VOLTAS", "WHIRLPOOL", "ZOMATO", "ABB",
]


def run_batch(tickers: list[str], dry_run: bool = False, save_json: bool = False) -> dict:
    """
    Processes a list of tickers through the Apify pipeline and writes to Supabase.

    Returns a summary dict with counts of successes, failures, and skipped.
    """
    results = {"success": [], "failed": [], "skipped": []}
    total = len(tickers)

    print(f"\n{'='*60}")
    print(f"  VERDIQ Apify Ingestion — {total} tickers")
    print(f"  Dry run: {dry_run}")
    print(f"{'='*60}\n")

    for i, ticker in enumerate(tickers, 1):
        print(f"[{i:>3}/{total}] Processing {ticker}...")

        try:
            # ── Step 1: Fetch + normalize from Apify ──
            fundamentals = ApifyScreenerAdapter.get_normalized_fundamentals(ticker)

            # ── Step 2 (optional): Save raw JSON for debugging ──
            if save_json:
                out_dir = Path(__file__).parent.parent.parent / "data" / "apify_cache"
                out_dir.mkdir(parents=True, exist_ok=True)
                json_path = out_dir / f"{ticker}.json"
                with open(json_path, "w") as f:
                    json.dump(fundamentals.model_dump(), f, indent=2, default=str)
                print(f"         Saved JSON → {json_path}")

            # ── Step 3: Log normalized pillars ──
            print(f"         P/E: {fundamentals.current_pe:.1f}x  |  "
                  f"ROE: {fundamentals.roe_percentage:.1f}%  |  "
                  f"D/E: {fundamentals.debt_to_equity:.2f}x  |  "
                  f"Rev Growth (3yr): {fundamentals.revenue_growth_3yr:.1f}%  |  "
                  f"Years: {len(fundamentals.history_years)}")

            # ── Step 4: Write to Supabase (skip in dry-run mode) ──
            if dry_run:
                print(f"         [DRY RUN] Skipping Supabase write.")
                results["skipped"].append(ticker)
            else:
                ok = SupabaseAdapter.write_historical_financials(fundamentals)
                if ok:
                    print(f"         ✅ Written to Supabase.")
                    results["success"].append(ticker)
                else:
                    print(f"         ❌ Supabase write failed.")
                    results["failed"].append(ticker)

        except Exception as e:
            print(f"         ❌ ERROR: {e}")
            results["failed"].append(ticker)

        # Rate-limit: be polite to Apify (avoid 429s)
        if i < total:
            time.sleep(1.5)

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"  Ingestion complete.")
    print(f"  ✅ Success : {len(results['success'])}")
    print(f"  ❌ Failed  : {len(results['failed'])}")
    print(f"  ⏭  Skipped : {len(results['skipped'])}")
    if results["failed"]:
        print(f"  Failed tickers: {', '.join(results['failed'])}")
    print(f"{'='*60}\n")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="VERDIQ Apify Batch Ingestion — pulls Screener.in data into Supabase."
    )
    parser.add_argument("--ticker", type=str, help="Single ticker (e.g. TATAMOTORS)")
    parser.add_argument(
        "--list",
        choices=["nifty50", "nifty100"],
        help="Ingest a preset list (nifty50 or nifty100)"
    )
    parser.add_argument("--file", type=str, help="Path to a file with one ticker per line")
    parser.add_argument("--dry-run", action="store_true", help="Parse + normalize but skip DB write")
    parser.add_argument("--save-json", action="store_true", help="Save each company's JSON to data/apify_cache/")

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

    run_batch(tickers, dry_run=args.dry_run, save_json=args.save_json)


if __name__ == "__main__":
    main()
