import os
import asyncio
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List
from dotenv import load_dotenv

from adapters.yfinance_adapter import YFinanceAdapter
from adapters.apify_screener import ApifyScreenerAdapter
from adapters.supabase_adapter import SupabaseAdapter
from services.scorecard_calculator import ScorecardCalculator
from services.valuation_engine import ValuationEngine


# Load environment variables from backend/.env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

app = FastAPI(
    title="VERDIQ API",
    description="Backend API powering the institutional-grade stock analyzer.",
    version="1.0"
)

# Enable CORS so the React/Streamlit frontend can fetch locally
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory warming lock: prevents duplicate API calls for the same ticker ──
# If two users search "ITC" simultaneously on a cold cache, only one pipeline
# runs. The second request gets a 202 "warming" response and retries.
_warming: set[str] = set()

def standardize_ticker(ticker: str) -> str:
    """Auto-appends .NS for Indian stocks if no exchange suffix is provided."""
    ticker = ticker.upper()
    if "." not in ticker and ticker.isalpha():
        return f"{ticker}.NS"
    return ticker


async def _run_pipeline(ticker: str) -> Dict[str, Any]:
    """
    Core data pipeline (shared by JIT and warm endpoints).
    Fetches from APIs, calculates, writes to Supabase, returns response dict.
    """
    # 1a. Company metadata (yfinance — free)
    info = YFinanceAdapter.get_info(ticker)
    company_name = info.get("longName", ticker)
    sector = info.get("sector", "Unknown Sector")

    # 1b. Deep fundamentals (Apify/Screener or mock fallback)
    apify_data = ApifyScreenerAdapter.get_deep_fundamentals(ticker)

    # 2. Calculate scorecard + valuation
    scorecard = ScorecardCalculator.calculate_scorecard(ticker, apify_data)
    valuation = await ValuationEngine.generate_valuation(ticker)

    scorecard_dict = scorecard.model_dump()
    valuation_dict = valuation.model_dump()

    # 3. Write to Supabase (so the next user gets the fast path)
    SupabaseAdapter.cache_full_analysis(
        ticker=ticker,
        company_name=company_name,
        sector=sector,
        scorecard_data=scorecard_dict,
        valuation_data=valuation_dict,
        apify_financials=apify_data,
    )

    return {
        "ticker": ticker,
        "company_name": company_name,
        "sector": sector,
        "last_price": valuation.current_price,
        "last_updated": "Real-time" if os.getenv("ANGEL_API_KEY") else "Delayed/Mocked",
        "scorecard": scorecard_dict,
        "valuation": valuation_dict,
    }


# ──────────────────────────────────────────────────────────────
# PRIMARY ENDPOINT: JIT Read-Through Cache
# ──────────────────────────────────────────────────────────────

@app.get("/api/v1/company/{ticker}")
async def get_company(ticker: str) -> Dict[str, Any]:
    """
    Just-In-Time (JIT) Read-Through Cache — the core of the VERDIQ data strategy.

    FAST PATH  (~0.2s): Supabase has fresh data (<24hrs) → return from DB instantly.
    SLOW PATH  (~6-8s): Cache miss → fetch Apify + yfinance + LLM → save to Supabase
                        → return result (future requests now hit the fast path).
    WARMING    (202):   Another request is already running the pipeline for this ticker.
                        Frontend should poll /health/cache/{ticker} and retry.

    The `_meta.served_from_cache` field tells the frontend which path was taken,
    so it can show/hide a loading skeleton or a "fresh data" badge.
    """
    try:
        ticker = standardize_ticker(ticker)

        # ─── FAST PATH: Check Supabase for fresh cache ───────────────────────
        cached = SupabaseAdapter.get_cached_analysis(ticker)
        if cached:
            cached["_meta"] = {
                "served_from_cache": True,
                "load_path": "fast",
                "cache_ttl_hours": 24,
            }
            return cached

        # ─── WARMING GUARD: Prevent duplicate concurrent pipeline runs ────────
        if ticker in _warming:
            # Another request is already building this ticker — tell frontend to poll
            return {
                "ticker": ticker,
                "_meta": {
                    "served_from_cache": False,
                    "load_path": "warming",
                    "message": (
                        f"{ticker} is being computed for the first time. "
                        "Poll GET /api/v1/health/cache/{ticker} and retry in ~8 seconds."
                    ),
                },
            }

        # ─── SLOW PATH: No fresh cache — run full JIT pipeline ───────────────
        print(f"[JIT] Cache miss — starting pipeline for {ticker}...")
        _warming.add(ticker)

        try:
            response = await _run_pipeline(ticker)
            response["_meta"] = {
                "served_from_cache": False,
                "load_path": "slow",
                "cache_ttl_hours": 24,
                "message": "First-time compute. This result is now cached for future requests.",
            }
            return response
        finally:
            _warming.discard(ticker)

    except Exception as e:
        _warming.discard(ticker)
        print(f"[ERROR] JIT pipeline failed for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/{ticker}/summary")
async def get_company_summary(ticker: str) -> Dict[str, Any]:
    """
    Returns a summary view for the requested company ticker.
    """
    ticker = standardize_ticker(ticker)
    company_data = await get_company(ticker)
    
    company_name = company_data.get("company_name", ticker)
    sector = company_data.get("sector", "Unknown Sector")
    
    from adapters.llm_client import LLMClientAdapter
    from adapters.yfinance_adapter import YFinanceAdapter
    try:
        info = YFinanceAdapter.get_info(ticker)
        description = info.get("longBusinessSummary", "")
        
        prompt = f"""
You are explaining {company_name} to a first-time investor in India who has never studied finance.
Company sector: {sector}
Official description: {description[:500]}

Write exactly 150 words following this structure:
1. What the company actually does in simple terms.
2. A strict breakdown of its Revenue Segments (e.g., "It makes 40% of its money from X, but Y is growing the fastest").
3. One key risk a new investor should know.

Rules: No jargon. No bullet points. One flowing paragraph. Conversational tone. Focus heavily on where the money comes from.
"""
        summary_text = await LLMClientAdapter.generate(prompt)
        
        revenue_growth_pct = company_data.get('scorecard', {}).get('revenue_growth', 0)
        profit_margin_pct = company_data.get('scorecard', {}).get('profit_margin', 0)
        debt_to_equity = company_data.get('scorecard', {}).get('debt_to_equity', 0)
        
        fallback_text = f"{company_name} is one of India's {sector} companies. " + (f"The company has shown {'+' if revenue_growth_pct > 0 else ''}{revenue_growth_pct:.1f}% revenue growth. " if revenue_growth_pct != 0 else "") + (f"It maintains a healthy profit margin of {profit_margin_pct:.1f}%. " if profit_margin_pct > 5 else f"Profit margins are currently thin at {profit_margin_pct:.1f}%, which is worth watching. " if profit_margin_pct > 0 else "") + (f"The company is practically debt-free with a D/E ratio of {debt_to_equity:.2f}. " if debt_to_equity < 50 else f"It carries significant debt at {debt_to_equity:.2f}x equity. ") + "Always research further before investing."

        if "unable" in summary_text.lower() or "unavailable" in summary_text.lower():
            summary_text = fallback_text
    except Exception:
        # Re-build fallback variables cleanly in case exception triggered before they were defined
        revenue_growth_pct = company_data.get('scorecard', {}).get('revenue_growth', 0)
        profit_margin_pct = company_data.get('scorecard', {}).get('profit_margin', 0)
        debt_to_equity = company_data.get('scorecard', {}).get('debt_to_equity', 0)
        fallback_text = f"{company_name} is one of India's {sector} companies. " + (f"The company has shown {'+' if revenue_growth_pct > 0 else ''}{revenue_growth_pct:.1f}% revenue growth. " if revenue_growth_pct != 0 else "") + (f"It maintains a healthy profit margin of {profit_margin_pct:.1f}%. " if profit_margin_pct > 5 else f"Profit margins are currently thin at {profit_margin_pct:.1f}%, which is worth watching. " if profit_margin_pct > 0 else "") + (f"The company is practically debt-free with a D/E ratio of {debt_to_equity:.2f}. " if debt_to_equity < 50 else f"It carries significant debt at {debt_to_equity:.2f}x equity. ") + "Always research further before investing."
        summary_text = fallback_text

    return {
        "ticker": company_data.get("ticker"),
        "company_name": company_name,
        "sector": sector,
        "last_price": company_data.get("last_price"),
        "scorecard": company_data.get("scorecard"),
        "valuation": company_data.get("valuation"),
        "summary": summary_text,
        "_meta": company_data.get("_meta"),
    }


# ──────────────────────────────────────────────────────────────
# BACKGROUND WARM ENDPOINT
# ──────────────────────────────────────────────────────────────

@app.post("/api/v1/company/{ticker}/warm")
async def warm_ticker(ticker: str, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    Triggers the pipeline for a ticker in the background — returns immediately (202).

    Use this to pre-warm stocks before a user actually requests them:
      - Call /warm for NIFTY 50 on app startup
      - Call /warm when a user hovers over a stock card (speculative pre-fetch)
      - Call /warm from a cron job every 12 hours for popular tickers

    The frontend polls GET /health/cache/{ticker} to know when data is ready.
    """
    ticker = standardize_ticker(ticker)

    # Skip if already fresh or already warming
    if SupabaseAdapter.is_cache_fresh(ticker):
        return {
            "ticker": ticker,
            "status": "already_fresh",
            "message": f"{ticker} cache is fresh. No warm needed.",
        }

    if ticker in _warming:
        return {
            "ticker": ticker,
            "status": "already_warming",
            "message": f"{ticker} is already being computed.",
        }

    async def _background_warm():
        _warming.add(ticker)
        try:
            print(f"[WARM] Background pipeline started for {ticker}...")
            await _run_pipeline(ticker)
            print(f"[WARM] ✅ {ticker} is now cached.")
        except Exception as e:
            print(f"[WARM] ❌ Failed to warm {ticker}: {e}")
        finally:
            _warming.discard(ticker)

    background_tasks.add_task(_background_warm)

    return {
        "ticker": ticker,
        "status": "warming",
        "message": (
            f"Pipeline started for {ticker} in the background. "
            "Poll GET /api/v1/health/cache/{ticker} to check when ready."
        ),
    }


# ──────────────────────────────────────────────────────────────
# UTILITY ENDPOINTS
# ──────────────────────────────────────────────────────────────

@app.get("/api/v1/companies")
def list_companies() -> List[Dict[str, Any]]:
    """
    Returns all active companies that have been JIT-cached in Supabase.
    Starts empty — grows organically as users search for stocks.
    Powers the search autocomplete dropdown.
    """
    return SupabaseAdapter.get_company_list()


@app.get("/api/v1/health/cache/{ticker}")
def check_cache_health(ticker: str) -> Dict[str, Any]:
    """
    Lightweight cache freshness probe — does NOT pull full data.
    Frontend polls this after receiving a 'warming' response to know when to retry.
    """
    ticker = standardize_ticker(ticker)
    is_fresh = SupabaseAdapter.is_cache_fresh(ticker)
    is_warming = ticker in _warming

    return {
        "ticker": ticker,
        "cache_fresh": is_fresh,
        "is_warming": is_warming,
        "ttl_hours": 24,
        "status": (
            "ready" if is_fresh
            else "warming" if is_warming
            else "cold"
        ),
        "message": (
            f"Data ready — serve from cache."
            if is_fresh else
            f"Pipeline running — retry in ~5 seconds."
            if is_warming else
            f"No data yet. Call GET /api/v1/company/{ticker} to trigger JIT fetch."
        ),
    }


@app.get("/api/v1/warming")
def list_warming() -> Dict[str, Any]:
    """
    Shows which tickers are currently being computed.
    Useful for debugging concurrent requests.
    """
    return {
        "warming_tickers": list(_warming),
        "count": len(_warming),
    }


@app.api_route("/health", methods=["GET", "HEAD"])
def health_check():
    return {"status": "ok", "message": "VERDIQ Backend is running."}

# FEATURE: LAYMAN BUSINESS BREAKDOWN (Removed logic combined into route above)


# ──────────────────────────────────────────────────────────────
# FEATURE: SMART MONEY TRACKER
# ──────────────────────────────────────────────────────────────

@app.get("/api/v1/company/{ticker}/smart-money")
async def get_smart_money(ticker: str) -> Dict[str, Any]:
    ticker = standardize_ticker(ticker)
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info

        promoter = round((info.get("heldPercentInsiders") or 0) * 100, 2)
        institution = round((info.get("heldPercentInstitutions") or 0) * 100, 2)
        retail = round(max(0, 100 - promoter - institution), 2)

        if institution >= 40:
            signal, signal_note = "bullish", "Strong institutional confidence — large funds are invested."
        elif institution >= 15:
            signal, signal_note = "neutral", "Moderate institutional interest — worth monitoring."
        else:
            signal, signal_note = "bearish", "Limited institutional holdings — mostly retail-driven stock."

        return {
            "ticker": ticker,
            "promoter_holding": promoter,
            "fii_holding": institution,
            "retail_holding": retail,
            "signal": signal,
            "signal_note": signal_note
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Smart money fetch failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
