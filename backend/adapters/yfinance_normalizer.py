"""
Strategy 2: yfinance + Pydantic Normalization Engine (100% Free)

Fetches P&L and Balance Sheet data from Yahoo Finance, normalizes the messy
column names into our clean Supabase schema, and outputs the same
NormalizedCompanyFundamentals model that the Apify pipeline uses.

Key design:
  - COLUMN_MAP normalizes Yahoo's inconsistent naming ("Total Revenue",
    "Operating Revenue", "Total Operating Income") → `total_revenue`
  - NaN values are gracefully handled (→ 0 or None)
  - Works for Indian stocks (.NS suffix auto-appended)
  - Usually returns 3-4 years of data (Yahoo's limit for most Indian stocks)

Usage:
    from backend.adapters.yfinance_normalizer import YFinanceNormalizer
    result = YFinanceNormalizer.fetch_and_normalize("TATAMOTORS")
"""

import math
import statistics
import yfinance as yf
import pandas as pd
from typing import Dict, Any, List, Optional

from backend.models.financials import NormalizedCompanyFundamentals, NormalizedYearFinancials


# ── Yahoo Finance column name normalization ──
# Yahoo uses different names across stocks and exchanges. We map ALL known
# variants to our single canonical name.
PNL_COLUMN_MAP = {
    # Revenue
    "Total Revenue": "total_revenue",
    "Operating Revenue": "total_revenue",
    "Total Operating Income": "total_revenue",
    "Revenue": "total_revenue",

    # Net Profit
    "Net Income": "net_profit",
    "Net Income From Continuing Operations": "net_profit",
    "Net Income Common Stockholders": "net_profit",

    # Operating Profit
    "Operating Income": "operating_profit",
    "EBIT": "operating_profit",
    "Operating Profit": "operating_profit",

    # EPS (sometimes in financials, sometimes in info)
    "Basic EPS": "eps",
    "Diluted EPS": "eps",
}

BS_COLUMN_MAP = {
    # Equity
    "Total Equity Gross Minority Interest": "total_equity",
    "Stockholders Equity": "total_equity",
    "Total Stockholder Equity": "total_equity",
    "Common Stock Equity": "total_equity",

    # Debt
    "Total Debt": "total_debt",
    "Long Term Debt": "total_debt",
    "Total Non Current Liabilities Net Minority Interest": "total_debt",
    "Net Debt": "total_debt",
}


class YFinanceNormalizer:
    """
    Fetches raw financials from yfinance, cleans & normalizes column names,
    fills NaN values, and outputs a NormalizedCompanyFundamentals model.
    """

    @staticmethod
    def _get_ns_ticker(symbol: str) -> str:
        """Ensure .NS suffix for NSE stocks."""
        if symbol.endswith(".NS") or symbol.endswith(".BO"):
            return symbol
        return f"{symbol}.NS"

    @classmethod
    def fetch_and_normalize(cls, ticker: str) -> NormalizedCompanyFundamentals:
        """
        Main entry point. Fetches all data from yfinance, normalizes it,
        and returns a typed Pydantic model ready for Supabase.
        """
        ns_ticker = cls._get_ns_ticker(ticker)
        stock = yf.Ticker(ns_ticker)

        # ── Fetch raw data ──
        info = stock.info or {}
        pnl_df = stock.financials          # Columns = dates, Rows = line items
        bs_df = stock.balance_sheet        # Same shape

        company_name = info.get("longName", info.get("shortName", ticker))
        sector = info.get("sector", "Unknown")
        current_pe = info.get("trailingPE", info.get("forwardPE", 0)) or 0

        # ── Normalize P&L ──
        pnl_data = cls._normalize_dataframe(pnl_df, PNL_COLUMN_MAP)

        # ── Normalize Balance Sheet ──
        bs_data = cls._normalize_dataframe(bs_df, BS_COLUMN_MAP)

        # ── Build year-by-year rows ──
        yearly: List[NormalizedYearFinancials] = []
        years: List[str] = []
        revenue_history: List[float] = []
        pm_history: List[float] = []
        pe_history: List[float] = []
        roe_history: List[float] = []
        de_history: List[float] = []

        # Sort dates oldest → newest
        all_dates = sorted(set(list(pnl_data.keys()) + list(bs_data.keys())))

        for date_key in all_dates:
            pnl = pnl_data.get(date_key, {})
            bs = bs_data.get(date_key, {})

            # Convert date to fiscal year label
            fy = cls._date_to_fy(date_key)
            years.append(fy)

            # Extract and clean values
            revenue = cls._safe(pnl.get("total_revenue"))
            # Convert from raw (₹) to Crores
            revenue_cr = round(revenue / 1e7, 2) if revenue else 0

            net_profit = cls._safe(pnl.get("net_profit"))
            net_profit_cr = round(net_profit / 1e7, 2) if net_profit else 0

            op_profit = cls._safe(pnl.get("operating_profit"))
            op_profit_cr = round(op_profit / 1e7, 2) if op_profit else 0

            eps = cls._safe(pnl.get("eps"))

            equity = cls._safe(bs.get("total_equity"))
            equity_cr = round(equity / 1e7, 2) if equity else 0

            debt = cls._safe(bs.get("total_debt"))
            debt_cr = round(debt / 1e7, 2) if debt else 0

            npm = round((net_profit / revenue * 100), 2) if revenue else 0
            de = round(debt / equity, 2) if equity else 0
            roe_yr = round((net_profit / equity * 100), 2) if equity else 0

            revenue_history.append(revenue_cr)
            pm_history.append(npm)
            de_history.append(de)
            roe_history.append(roe_yr)
            pe_history.append(current_pe)  # yfinance only gives current PE

            yearly.append(NormalizedYearFinancials(
                ticker=ticker,
                fiscal_year=fy,
                total_revenue=revenue_cr,
                net_profit=net_profit_cr,
                operating_profit=op_profit_cr,
                net_profit_margin=npm,
                total_equity=equity_cr,
                total_debt=debt_cr,
                debt_to_equity=de,
                roe=roe_yr,
                pe_ratio=current_pe,
                eps=eps,
                source="yfinance",
            ))

        # ── Aggregated metrics ──
        avg_pe = round(statistics.mean(pe_history), 2) if pe_history else current_pe
        rev_growth = 0.0
        if len(revenue_history) >= 4 and revenue_history[0] > 0:
            rev_growth = round(((revenue_history[-1] / revenue_history[0]) - 1) * 100, 2)
        elif len(revenue_history) >= 2 and revenue_history[0] > 0:
            rev_growth = round(((revenue_history[-1] / revenue_history[0]) - 1) * 100, 2)

        return NormalizedCompanyFundamentals(
            ticker=ticker,
            company_name=company_name,
            sector=sector,
            current_pe=round(current_pe, 2),
            historical_pe_5yr=avg_pe,
            sector_pe=info.get("sectorPE", avg_pe),
            roe_percentage=roe_history[-1] if roe_history else 0,
            debt_to_equity=de_history[-1] if de_history else 0,
            revenue_growth_3yr=rev_growth,
            net_profit_margin=pm_history[-1] if pm_history else 0,
            pe_history=pe_history,
            roe_history=roe_history,
            de_history=de_history,
            revenue_history=revenue_history,
            profit_margin_history=pm_history,
            history_years=years,
            yearly_financials=yearly,
            source="yfinance",
        )

    # ──────────────────────────────────────────────
    # INTERNAL: DataFrame normalization
    # ──────────────────────────────────────────────

    @classmethod
    def _normalize_dataframe(cls, df: pd.DataFrame, column_map: Dict[str, str]) -> Dict[str, Dict[str, float]]:
        """
        Takes a yfinance DataFrame (rows = line items, cols = dates),
        transposes it, normalizes column names, and returns:
            { "2024-03-31": {"total_revenue": 12345, "net_profit": 678}, ... }
        """
        if df is None or df.empty:
            return {}

        result = {}

        # yfinance financials: index = line item names, columns = dates
        for date_col in df.columns:
            date_key = str(date_col.date()) if hasattr(date_col, 'date') else str(date_col)
            row_data: Dict[str, float] = {}

            for idx_name in df.index:
                canonical = column_map.get(str(idx_name))
                if canonical:
                    val = df.loc[idx_name, date_col]
                    # Only take the first mapping (avoid overwriting total_revenue with a worse match)
                    if canonical not in row_data:
                        row_data[canonical] = cls._safe(val)

            result[date_key] = row_data

        return result

    @staticmethod
    def _safe(val: Any) -> float:
        """Convert any value to float. Returns 0.0 for NaN/None/errors."""
        if val is None:
            return 0.0
        try:
            f = float(val)
            return 0.0 if (math.isnan(f) or math.isinf(f)) else f
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _date_to_fy(date_str: str) -> str:
        """
        Converts '2024-03-31' → 'FY24'.
        Indian fiscal years end in March, so March 2024 = FY24.
        """
        try:
            parts = date_str.split("-")
            year = int(parts[0])
            month = int(parts[1])
            # Indian FY: April to March. If month <= 3, it's the ending FY
            if month <= 3:
                fy_year = year % 100
            else:
                fy_year = (year + 1) % 100
            return f"FY{fy_year:02d}"
        except (ValueError, IndexError):
            return date_str
