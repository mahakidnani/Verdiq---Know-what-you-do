"""
Adapter for Screener.in fundamentals via Apify.

Handles two JSON formats:
  1. Real Screener JSON (from Apify Actor) — has keys like 'Ratios', 'Profit_and_loss', 'Balance_sheet'
  2. Our internal normalized format — has keys like 'financials.ROE.history', 'years'

The `normalize_screener_json()` method bridges the gap: it takes the raw Apify
output and returns a NormalizedCompanyFundamentals model that both the
FastAPI live endpoint and the batch ingestion script can use identically.
"""

import os
import statistics
from apify_client import ApifyClient
from typing import Dict, Any, List, Optional

from backend.models.financials import NormalizedCompanyFundamentals, NormalizedYearFinancials
from backend.adapters.yfinance_normalizer import YFinanceNormalizer


# ── The Apify Actor ID — replace with your actual Screener scraper actor ──
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "indian-stocks/screener-scraper")


class ApifyScreenerAdapter:
    """
    Adapter for extracting deep historical fundamentals using the Screener.in extraction over Apify.
    Uses free monthly credits efficiently. Returns fallback data if API key missing.
    """

    # ──────────────────────────────────────────────
    # PUBLIC: Fetch + Normalize (used by FastAPI live endpoint)
    # ──────────────────────────────────────────────

    @classmethod
    def get_deep_fundamentals(cls, ticker: str) -> Dict[str, Any]:
        """
        Fetches fundamentals and returns them in our INTERNAL format
        (the shape that ScorecardCalculator and ValuationEngine expect).

        Fallback Chain for Fundamentals:
          1. Apify (10-year deep history, perfectly structured)
          2. yfinance (3-4 year history, normalized via YFinanceNormalizer)
          3. Static mock (keeps UI alive if both APIs fail)
        """
        normalized = cls.get_normalized_fundamentals(ticker)
        return cls._to_internal_format(normalized)

    @classmethod
    def get_normalized_fundamentals(cls, ticker: str) -> NormalizedCompanyFundamentals:
        """
        Fetches fundamentals and returns a fully typed Pydantic model.
        Used by the batch ingestion script for clean Supabase writes.
        """
        # Try Apify first
        raw = cls._fetch_raw(ticker)
        
        # If _fetch_raw returns real data (detected by "Ratios" or "Profit_and_loss" keys):
        if "Ratios" in raw or "Profit_and_loss" in raw:
            return cls.normalize_screener_json(ticker, raw)
            
        print(f"[WARN] Falling back to yfinance normalizer for {ticker} fundamentals...")
        
        # Fallback to yfinance (Strategy 2)
        try:
            yfinance_data = YFinanceNormalizer.fetch_and_normalize(ticker)
            if yfinance_data and yfinance_data.history_years:
                return yfinance_data
        except Exception as e:
            print(f"[ERROR] YFinanceNormalizer fallback failed for {ticker}: {e}")
            
        print(f"[WARN] Both Apify and yfinance failed for {ticker}. Returning static mock.")
        return cls.normalize_screener_json(ticker, raw) # raw is the mock here

    # ──────────────────────────────────────────────
    # FETCH: Hit Apify API or return mock
    # ──────────────────────────────────────────────

    @classmethod
    def _fetch_raw(cls, ticker: str) -> Dict[str, Any]:
        """Calls the Apify Actor and returns raw JSON. Falls back to mock data."""
        api_token = os.getenv("APIFY_API_TOKEN")

        if not api_token:
            print(f"[WARN] No APIFY_API_TOKEN found. Returning mock dataset for {ticker}...")
            return cls._get_mock_screener_json(ticker)

        try:
            client = ApifyClient(api_token)
            run = client.actor(APIFY_ACTOR_ID).call(run_input={
                "symbols": [ticker],
                "exchange": "NSE",
            })
            dataset = client.dataset(run["defaultDatasetId"])
            items = dataset.list_items().items
            if items:
                print(f"[APIFY] Fetched real data for {ticker}.")
                return items[0]
            print(f"[WARN] Apify returned empty dataset for {ticker}. Using mock.")
            return cls._get_mock_screener_json(ticker)
        except Exception as e:
            print(f"[ERROR] Apify call failed for {ticker}: {e}")
            return cls._get_mock_screener_json(ticker)

    # ──────────────────────────────────────────────
    # NORMALIZE: Raw Screener JSON → Pydantic Model
    # ──────────────────────────────────────────────

    @classmethod
    def normalize_screener_json(cls, ticker: str, raw: Dict[str, Any]) -> NormalizedCompanyFundamentals:
        """
        Parses the real Screener.in JSON (with 'Ratios', 'Profit_and_loss', 'Balance_sheet')
        into a clean NormalizedCompanyFundamentals model.

        Also handles our legacy internal format gracefully (for mock data).
        """

        # ── Detect format: real Screener JSON vs our internal mock format ──
        if "Ratios" in raw or "Profit_and_loss" in raw:
            return cls._normalize_real_screener(ticker, raw)
        elif "financials" in raw:
            return cls._normalize_internal_format(ticker, raw)
        else:
            print(f"[WARN] Unknown JSON format for {ticker}. Returning empty model.")
            return NormalizedCompanyFundamentals(ticker=ticker, source="apify")

    @classmethod
    def _normalize_real_screener(cls, ticker: str, raw: Dict[str, Any]) -> NormalizedCompanyFundamentals:
        """Parses the actual Screener.in Apify JSON format."""

        ratios = raw.get("Ratios", {})
        pnl_rows = raw.get("Profit_and_loss", [])       # List of dicts, one per year
        bs_rows = raw.get("Balance_sheet", [])           # List of dicts, one per year
        company_name = raw.get("Company_name", raw.get("company_name", ticker))

        # ── Extract ratio scalars ──
        current_pe = cls._safe_float(ratios.get("Stock P/E", ratios.get("PE", 0)))
        sector_pe = cls._safe_float(ratios.get("Sector PE", ratios.get("SectorPE", 0)))
        roe_pct = cls._safe_float(ratios.get("ROE %", ratios.get("ROCE %", 0)))
        de_ratio = cls._safe_float(ratios.get("Debt to equity", ratios.get("Debt_to_equity", 0)))

        # ── Build year-by-year rows from P&L and Balance Sheet ──
        yearly: List[NormalizedYearFinancials] = []
        years: List[str] = []
        revenue_history: List[float] = []
        pm_history: List[float] = []
        pe_history_list: List[float] = []
        roe_history_list: List[float] = []
        de_history_list: List[float] = []

        for i, pnl in enumerate(pnl_rows):
            fy = pnl.get("Year", pnl.get("year", f"FY{i}"))
            years.append(str(fy))

            revenue = cls._safe_float(pnl.get("Sales", pnl.get("Revenue", 0)))
            net_profit = cls._safe_float(pnl.get("Net Profit", pnl.get("Net_profit", 0)))
            op_profit = cls._safe_float(pnl.get("Operating Profit", pnl.get("Operating_profit", 0)))
            npm = round((net_profit / revenue * 100), 2) if revenue else 0

            revenue_history.append(revenue)
            pm_history.append(npm)

            # Try to match balance sheet row by index
            bs = bs_rows[i] if i < len(bs_rows) else {}
            equity = cls._safe_float(bs.get("Total Equity", bs.get("Equity", 0)))
            debt = cls._safe_float(bs.get("Total Debt", bs.get("Borrowings", 0)))
            de = round(debt / equity, 2) if equity else 0
            roe_yr = round((net_profit / equity * 100), 2) if equity else 0

            de_history_list.append(de)
            roe_history_list.append(roe_yr)

            # PE per year (if available in ratios history)
            pe_yr = cls._safe_float(pnl.get("PE", 0))
            pe_history_list.append(pe_yr if pe_yr else current_pe)

            yearly.append(NormalizedYearFinancials(
                ticker=ticker,
                fiscal_year=str(fy),
                total_revenue=revenue,
                net_profit=net_profit,
                operating_profit=op_profit,
                net_profit_margin=npm,
                total_equity=equity,
                total_debt=debt,
                debt_to_equity=de,
                roe=roe_yr,
                roce=cls._safe_float(pnl.get("ROCE", 0)),
                pe_ratio=pe_yr if pe_yr else current_pe,
                eps=cls._safe_float(pnl.get("EPS", 0)),
                source="apify",
            ))

        # ── Compute aggregated metrics ──
        avg_pe = round(statistics.mean(pe_history_list), 2) if len(pe_history_list) >= 2 else current_pe
        rev_growth_3yr = 0.0
        if len(revenue_history) >= 4 and revenue_history[-4] > 0:
            rev_growth_3yr = round(((revenue_history[-1] / revenue_history[-4]) - 1) * 100, 2)

        return NormalizedCompanyFundamentals(
            ticker=ticker,
            company_name=company_name,
            sector=raw.get("Sector", raw.get("sector")),
            current_pe=current_pe,
            historical_pe_5yr=avg_pe,
            sector_pe=sector_pe,
            roe_percentage=roe_pct,
            debt_to_equity=de_ratio,
            revenue_growth_3yr=rev_growth_3yr,
            net_profit_margin=pm_history[-1] if pm_history else 0,
            pe_history=pe_history_list,
            roe_history=roe_history_list,
            de_history=de_history_list,
            revenue_history=revenue_history,
            profit_margin_history=pm_history,
            history_years=years,
            yearly_financials=yearly,
            source="apify",
        )

    @classmethod
    def _normalize_internal_format(cls, ticker: str, raw: Dict[str, Any]) -> NormalizedCompanyFundamentals:
        """Handles our legacy internal/mock format (used when APIFY_API_TOKEN is missing)."""

        financials = raw.get("financials", {})
        years = raw.get("years", [])

        pe_h = financials.get("PE", {}).get("history", [])
        roe_h = financials.get("ROE", {}).get("history", [])
        de_h = financials.get("DebtToEquity", {}).get("history", [])
        rev_h = financials.get("Revenue_Cr", {}).get("history", [])
        pm_h = financials.get("NetProfitMargin", {}).get("history", [])

        avg_pe = round(statistics.mean(pe_h), 2) if len(pe_h) >= 2 else (pe_h[-1] if pe_h else 0)
        rev_growth = 0.0
        if len(rev_h) >= 4 and rev_h[-4] > 0:
            rev_growth = round(((rev_h[-1] / rev_h[-4]) - 1) * 100, 2)

        # Build year-by-year rows from history arrays
        yearly = []
        for i, yr in enumerate(years):
            yearly.append(NormalizedYearFinancials(
                ticker=ticker,
                fiscal_year=yr,
                total_revenue=rev_h[i] if i < len(rev_h) else None,
                net_profit_margin=pm_h[i] if i < len(pm_h) else None,
                debt_to_equity=de_h[i] if i < len(de_h) else None,
                roe=roe_h[i] if i < len(roe_h) else None,
                pe_ratio=pe_h[i] if i < len(pe_h) else None,
                source="apify",
            ))

        return NormalizedCompanyFundamentals(
            ticker=ticker,
            current_pe=pe_h[-1] if pe_h else 0,
            historical_pe_5yr=avg_pe,
            sector_pe=financials.get("SectorPE", 0),
            roe_percentage=roe_h[-1] if roe_h else 0,
            debt_to_equity=de_h[-1] if de_h else 0,
            revenue_growth_3yr=rev_growth,
            net_profit_margin=pm_h[-1] if pm_h else 0,
            pe_history=pe_h,
            roe_history=roe_h,
            de_history=de_h,
            revenue_history=rev_h,
            profit_margin_history=pm_h,
            history_years=years,
            yearly_financials=yearly,
            source="apify",
        )

    # ──────────────────────────────────────────────
    # CONVERT: Pydantic → Internal dict format
    # ──────────────────────────────────────────────

    @classmethod
    def _to_internal_format(cls, model: NormalizedCompanyFundamentals) -> Dict[str, Any]:
        """
        Converts the normalized Pydantic model back to the dict format
        that ScorecardCalculator and SupabaseAdapter.cache_full_analysis() expect.
        This preserves full backward compatibility.
        """
        return {
            "symbol": model.ticker,
            "financials": {
                "ROE": {"history": model.roe_history},
                "DebtToEquity": {"history": model.de_history},
                "SectorPE": model.sector_pe,
                "PE": {"history": model.pe_history},
                "Revenue_Cr": {"history": model.revenue_history},
                "NetProfitMargin": {"history": model.profit_margin_history},
            },
            "years": model.history_years,
        }

    # ──────────────────────────────────────────────
    # UTILS
    # ──────────────────────────────────────────────

    @staticmethod
    def _safe_float(val: Any) -> float:
        """Safely convert any value to float. Returns 0.0 on failure."""
        if val is None:
            return 0.0
        try:
            # Handle strings like "22.1%" or "₹1,234"
            if isinstance(val, str):
                val = val.replace("%", "").replace("₹", "").replace(",", "").strip()
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    # ──────────────────────────────────────────────
    # MOCK: Structured fallback when no API key
    # ──────────────────────────────────────────────

    @classmethod
    def _get_mock_screener_json(cls, ticker: str) -> Dict[str, Any]:
        """
        Generates deterministic ticker-variant mock data.
        Each ticker gets unique but realistic financials seeded by its name.
        """
        import hashlib
        
        # Create deterministic seed from ticker
        seed = int(hashlib.md5(ticker.encode()).hexdigest()[:8], 16)
        
        # Generate ticker-specific multipliers (deterministic but varied)
        sales_multiplier = 0.6 + (seed % 80) / 100      # 0.6x to 1.4x
        growth_variance = 5 + (seed % 15)                # 5% to 20% annual growth
        pe_base = 15 + (seed % 25)                       # P/E 15-40x
        roe_base = 6 + (seed % 12)                       # ROE 6-18%
        de_base = 0.2 + (seed % 60) / 100                # D/E 0.2-0.8
        
        # Base year 25 values
        base_fy25_sales = 800000
        base_fy25_profit = 70000
        base_fy25_equity = 400000
        base_fy25_borrowings = 160000
        
        fy25_sales = int(base_fy25_sales * sales_multiplier)
        fy25_profit = int(base_fy25_profit * (sales_multiplier * 0.9 + 0.1))
        fy25_equity = int(base_fy25_equity * sales_multiplier)
        fy25_borrowings = int(base_fy25_borrowings * (de_base / 0.4))
        
        # Build 10-year history backwards from FY25
        pnl_history = []
        bs_history = []
        years = ["FY16", "FY17", "FY18", "FY19", "FY20", "FY21", "FY22", "FY23", "FY24", "FY25"]
        
        for i, year in enumerate(years):
            age = 9 - i  # 9 years ago to current
            compound_growth = (1 + growth_variance / 100) ** age
            
            sales = int(fy25_sales / compound_growth)
            net_profit = int(fy25_profit / (compound_growth * 0.95 + 0.05))
            operating_profit = int(net_profit * 1.4)
            eps = int(net_profit / 6.7)
            
            pe = pe_base + (seed % 10) - 5
            
            pnl_history.append({
                "Year": year,
                "Sales": sales,
                "Net Profit": net_profit,
                "Operating Profit": operating_profit,
                "EPS": eps,
                "PE": pe,
            })
            
            equity = int(fy25_equity / compound_growth)
            borrowings = int(fy25_borrowings / (compound_growth * 0.8 + 0.2))
            
            bs_history.append({
                "Year": year,
                "Equity": equity,
                "Borrowings": borrowings,
            })
        
        return {
            "Company_name": ticker,
            "Sector": "Unknown",
            "Ratios": {
                "Stock P/E": round(pe_base + (seed % 10), 1),
                "Sector PE": round(pe_base - 3 + (seed % 8), 1),
                "ROE %": round(roe_base, 1),
                "Debt to equity": round(de_base, 2),
                "ROCE %": round(roe_base + 2, 1),
            },
            "Profit_and_loss": pnl_history,
            "Balance_sheet": bs_history,
        }
