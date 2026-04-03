import yfinance as yf
import pandas as pd
from typing import Dict, Any, Optional

class YFinanceAdapter:
    """
    Adapter for fetching historical prices, basic fundamentals, and 52-week highs/lows
    using the completely free yfinance library.
    """
    
    @staticmethod
    def get_ticker(symbol: str) -> str:
        # Append .NS for National Stock Exchange if not present for Indian context
        return symbol if symbol.endswith(".NS") or symbol.endswith(".BO") else f"{symbol}.NS"
    
    @classmethod
    def get_info(cls, ticker: str) -> Dict[str, Any]:
        """Fetches basic profile, trailing P/E, 52wk highs/lows."""
        t = yf.Ticker(cls.get_ticker(ticker))
        return t.info

    @classmethod
    def get_history(cls, ticker: str, period: str = "5y") -> pd.DataFrame:
        """Fetches historical daily close prices to compute long-term trends and moving averages."""
        t = yf.Ticker(cls.get_ticker(ticker))
        # Ensure we drop any timezone weirdness if needed and keep close prices
        hist = t.history(period=period)
        return hist if not hist.empty else pd.DataFrame()

    @classmethod
    def get_financials(cls, ticker: str) -> pd.DataFrame:
        """Fetches historical income statements (Revenues, Profits). Usually 3-4 years available."""
        t = yf.Ticker(cls.get_ticker(ticker))
        return t.financials

    @classmethod
    def get_balance_sheet(cls, ticker: str) -> pd.DataFrame:
        """Fetches historical balance sheets to derive Total Debt and Total Equity."""
        t = yf.Ticker(cls.get_ticker(ticker))
        return t.balance_sheet

