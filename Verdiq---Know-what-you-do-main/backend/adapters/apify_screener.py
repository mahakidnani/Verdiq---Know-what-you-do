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
        Returns a mock payload in the REAL Screener JSON format.
        This is what the actual Apify Actor would return.
        """
        return {
            "Company_name": ticker,
            "Sector": "Unknown",
            "Ratios": {
                "Stock P/E": 27.4,
                "Sector PE": 22.1,
                "ROE %": 9.4,
                "Debt to equity": 0.43,
                "ROCE %": 12.1,
            },
            "Profit_and_loss": [
                {"Year": "FY16", "Sales": 396288, "Net Profit": 16247, "Operating Profit": 42350, "EPS": 24.3, "PE": 14.2},
                {"Year": "FY17", "Sales": 441345, "Net Profit": 21185, "Operating Profit": 48920, "EPS": 31.7, "PE": 16.8},
                {"Year": "FY18", "Sales": 539238, "Net Profit": 28040, "Operating Profit": 58210, "EPS": 41.9, "PE": 18.4},
                {"Year": "FY19", "Sales": 622809, "Net Profit": 37991, "Operating Profit": 72390, "EPS": 56.8, "PE": 22.1},
                {"Year": "FY20", "Sales": 658651, "Net Profit": 38201, "Operating Profit": 75100, "EPS": 57.2, "PE": 20.8},
                {"Year": "FY21", "Sales": 721634, "Net Profit": 45439, "Operating Profit": 85430, "EPS": 67.9, "PE": 23.4},
                {"Year": "FY22", "Sales": 792756, "Net Profit": 54720, "Operating Profit": 98200, "EPS": 81.8, "PE": 25.9},
                {"Year": "FY23", "Sales": 830024, "Net Profit": 58920, "Operating Profit": 102340, "EPS": 88.1, "PE": 24.1},
                {"Year": "FY24", "Sales": 879864, "Net Profit": 64230, "Operating Profit": 112560, "EPS": 96.0, "PE": 25.3},
                {"Year": "FY25", "Sales": 902468, "Net Profit": 71295, "Operating Profit": 120890, "EPS": 106.6, "PE": 27.4},
            ],
            "Balance_sheet": [
                {"Year": "FY16", "Equity": 160800, "Borrowings": 180096},
                {"Year": "FY17", "Equity": 175200, "Borrowings": 183960},
                {"Year": "FY18", "Equity": 198400, "Borrowings": 194432},
                {"Year": "FY19", "Equity": 225600, "Borrowings": 200784},
                {"Year": "FY20", "Equity": 248000, "Borrowings": 203360},
                {"Year": "FY21", "Equity": 280400, "Borrowings": 207496},
                {"Year": "FY22", "Equity": 310500, "Borrowings": 211140},
                {"Year": "FY23", "Equity": 342000, "Borrowings": 201780},
                {"Year": "FY24", "Equity": 378200, "Borrowings": 208010},
                {"Year": "FY25", "Equity": 412600, "Borrowings": 177418},
            ],
        }
