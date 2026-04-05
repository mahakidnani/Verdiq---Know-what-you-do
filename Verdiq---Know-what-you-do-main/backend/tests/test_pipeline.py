import asyncio
import os
from dotenv import load_dotenv

# Load .env from the backend directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

from backend.adapters.apify_screener import ApifyScreenerAdapter
from backend.adapters.supabase_adapter import SupabaseAdapter
from backend.services.scorecard_calculator import ScorecardCalculator
from backend.services.valuation_engine import ValuationEngine


async def run_pipeline():
    ticker = "RELIANCE"
    print(f"{'='*60}")
    print(f"  VERDIQ Pipeline Test — {ticker}")
    print(f"{'='*60}\n")

    # ── Phase 1: Check Supabase Cache ──
    print("── Phase 1: Checking Supabase cache...")
    cached = SupabaseAdapter.get_cached_analysis(ticker)
    if cached:
        print(f"✅ FAST PATH — Served from Supabase cache!")
        print(f"   Company: {cached['company_name']}")
        print(f"   Sector:  {cached['sector']}")
        print(f"   Price:   ₹{cached['last_price']}")
        print(f"   Verdict: {cached['valuation']['verdict'].upper()}")
        print(f"   Cached:  {cached['last_updated']}")
        return

    # ── Phase 2: Slow Path — Fetch & Calculate ──
    print("❌ Cache MISS — Running full pipeline...\n")

    print("── Phase 2a: Fetching fundamentals...")
    apify_data = ApifyScreenerAdapter.get_deep_fundamentals(ticker)
    print(f"   Got {len(apify_data.get('years', []))} years of data.\n")

    print("── Phase 2b: Running Scorecard Calculator...")
    scorecard = ScorecardCalculator.calculate_scorecard(ticker)
    sc = scorecard.model_dump()
    print(f"   Revenue:       ₹{sc['revenue']['value']:,.0f} Cr ({sc['revenue']['health']})")
    print(f"   Profit Margin: {sc['profit_margin']['value']}% ({sc['profit_margin']['health']})")
    print(f"   ROE:           {sc['roe']['value']}% ({sc['roe']['health']})")
    print(f"   Debt/Equity:   {sc['debt_equity']['value']}x ({sc['debt_equity']['health']})")
    print(f"   P/E vs Sector: {sc['pe_ratio']['value']}x ({sc['pe_ratio']['health']})\n")

    print("── Phase 2c: Running Valuation Engine...")
    valuation = await ValuationEngine.generate_valuation(ticker)
    vd = valuation.model_dump()
    print(f"   Verdict:    {vd['verdict'].upper()}")
    print(f"   Confidence: {vd['confidence']} ({vd['confidence_score']}%)")
    print(f"   Rationale:  {vd['rationale'][:80]}...")
    for s in vd['signals']:
        icon = "🔴" if s['bearing'] == "bearish" else ("🟢" if s['bearing'] == "bullish" else "⚪")
        print(f"   {icon} {s['label']}: {s['value']} — {s['note']}")
    print()

    # ── Phase 3: Write to Supabase ──
    print("── Phase 3: Caching to Supabase...")
    success = SupabaseAdapter.cache_full_analysis(
        ticker=ticker,
        company_name="Reliance Industries Ltd",
        sector="Energy",
        scorecard_data=sc,
        valuation_data=vd,
        apify_financials=apify_data,
    )
    if success:
        print("✅ Successfully written to Supabase!\n")

    # ── Phase 4: Verify cache now works ──
    print("── Phase 4: Re-checking cache (should be a HIT now)...")
    cached2 = SupabaseAdapter.get_cached_analysis(ticker)
    if cached2:
        print(f"✅ VERIFIED — Cache is working. Verdict from DB: {cached2['valuation']['verdict'].upper()}")
    else:
        print("⚠️  Cache verification failed. Check SUPABASE_URL and SUPABASE_KEY in .env")

    print(f"\n{'='*60}")
    print("  Pipeline test complete.")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
