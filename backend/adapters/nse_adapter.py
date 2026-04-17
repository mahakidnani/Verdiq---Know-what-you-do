"""
NSE India data adapter using PKNSETools.

VERDIQ's second-line fallback for price and market data.

Priority chain in live_broker.py:
  1. Angel One SmartAPI   → live_broker._from_angel()   (real-time, needs broker auth)
  2. PKNSETools Benny/NSE → nse_adapter.py              ← THIS FILE (NSE official, free)
  3. yfinance             → live_broker._from_yfinance() (global free fallback)
  4. Static mock                                         (last resort)

Key use cases for PKNSETools in VERDIQ:
  - Mid-cap / small-cap stocks that yfinance returns empty DataFrames for
  - Official NSE LTP without needing a broker login
  - Live NIFTY index constituent lists (so batch scripts stay current)
  - Intraday price series during market hours

Does NOT provide:
  - Multi-year P&L / Balance Sheet (use Apify or yfinance_normalizer)

Install: pip install PKNSETools pytz
"""

import math
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd


class NSEAdapter:
    """
    Thin, safe wrapper around PKNSETools using the actual installed API surface.

    All methods are ImportError-safe so the pipeline continues even when
    PKNSETools is not installed in a dev environment.

    Verified against installed package (PKNSETools v1.x):
      - nseStockDataFetcher  in PKNSETools.PKNSEStockDataFetcher
      - NSE                  in PKNSETools.Benny.NSE
      - Intra_Day            in PKNSETools.PKIntraDay
    """

    # ──────────────────────────────────────────────
    # QUOTE: Real-time LTP from NSE official API
    # ──────────────────────────────────────────────

    @classmethod
    def get_quote(cls, ticker: str) -> Dict[str, Any]:
        """
        Fetches the live LTP and price metadata from NSE's Benny API.

        Returns dict with: ltp, change, pchange, prev_close, volume, source.
        Returns empty dict on any failure — caller falls through to next source.
        """
        try:
            from PKNSETools.Benny.NSE import NSE  # type: ignore

            with tempfile.TemporaryDirectory() as tmpdir:
                nse = NSE(download_folder=tmpdir)
                raw = nse.quote(ticker.upper())

            price_info = raw.get("priceInfo", {})
            intraday_hl = price_info.get("intraDayHighLow", {})

            ltp = float(price_info.get("lastPrice", 0) or 0)
            if ltp == 0:
                return {}

            return {
                "ticker": ticker.upper(),
                "ltp": ltp,
                "change": float(price_info.get("change", 0) or 0),
                "pchange": float(price_info.get("pChange", 0) or 0),
                "high": float(intraday_hl.get("max", 0) or 0),
                "low": float(intraday_hl.get("min", 0) or 0),
                "prev_close": float(price_info.get("previousClose", 0) or 0),
                "volume": int(
                    raw.get("marketDeptOrderBook", {}).get("totalTradedVolume", 0) or 0
                ),
                "source": "nse_official",
            }

        except ImportError:
            print("[WARN] PKNSETools not installed. Run: pip install PKNSETools")
            return {}
        except Exception as e:
            print(f"[WARN] NSEAdapter.get_quote failed for {ticker}: {e}")
            return {}

    # ──────────────────────────────────────────────
    # OHLCV HISTORY: Daily closing prices
    # ──────────────────────────────────────────────

    @classmethod
    def get_price_history(
        cls,
        ticker: str,
        period: str = "1y",
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        Fetches OHLCV history from NSE (up to ~3 years).

        This is the primary VERDIQ use case: when yfinance returns an empty
        DataFrame for an Indian mid-cap, call this instead.

        Args:
            ticker   : NSE symbol without suffix (e.g. "TATAMOTORS")
            period   : "1d","5d","1mo","3mo","6mo","1y","2y","5y","max"
            interval : "1d","1wk","1mo" for historical; "1m","5m" for intraday

        Returns:
            pd.DataFrame with Open, High, Low, Close, Volume columns.
            Empty DataFrame on failure.
        """
        try:
            from PKNSETools.PKNSEStockDataFetcher import nseStockDataFetcher  # type: ignore

            fetcher = nseStockDataFetcher()
            df = fetcher.fetchStockData(
                stockCode=ticker.upper(),
                period=period,
                interval=interval,
            )
            if df is not None and not df.empty:
                return df
            print(f"[WARN] PKNSETools returned empty history for {ticker}.")
            return pd.DataFrame()

        except ImportError:
            print("[WARN] PKNSETools not installed. Run: pip install PKNSETools")
            return pd.DataFrame()
        except Exception as e:
            print(f"[WARN] NSEAdapter.get_price_history failed for {ticker}: {e}")
            return pd.DataFrame()

    # ──────────────────────────────────────────────
    # REAL-TIME OHLCV (during market hours)
    # ──────────────────────────────────────────────

    @classmethod
    def get_realtime_ohlcv(cls, ticker: str) -> Dict[str, Any]:
        """
        Fetches real-time OHLCV using nseStockDataFetcher.getRealtimeOHLCV().
        Only meaningful during market hours (9:15 AM – 3:30 PM IST).
        """
        try:
            from PKNSETools.PKNSEStockDataFetcher import nseStockDataFetcher  # type: ignore

            fetcher = nseStockDataFetcher()
            data = fetcher.getRealtimeOHLCV(ticker.upper())
            if data:
                return {**data, "source": "nse_realtime"}
            return {}

        except ImportError:
            print("[WARN] PKNSETools not installed. Run: pip install PKNSETools")
            return {}
        except Exception as e:
            print(f"[WARN] NSEAdapter.get_realtime_ohlcv failed for {ticker}: {e}")
            return {}

    # ──────────────────────────────────────────────
    # 52-WEEK HIGH / LOW (computed from 1y history)
    # ──────────────────────────────────────────────

    @classmethod
    def get_52week_range(cls, ticker: str) -> Dict[str, float]:
        """
        Computes 52-week high and low from 1-year daily OHLCV history.
        Falls back to empty dict so the caller can try yfinance instead.
        """
        df = cls.get_price_history(ticker, period="1y", interval="1d")

        if df.empty or "High" not in df.columns or "Low" not in df.columns:
            return {}

        return {
            "week_52_high": float(df["High"].max()),
            "week_52_low": float(df["Low"].min()),
            "source": "nse_official",
        }

    # ──────────────────────────────────────────────
    # INDEX CONSTITUENTS: Live NIFTY lists from NSE
    # ──────────────────────────────────────────────

    # Index name → Benny allIndices key map
    _INDEX_NAME_MAP: dict = {
        1:  "NIFTY 50",
        2:  "NIFTY NEXT 50",
        4:  "NIFTY 100",
        5:  "NIFTY 200",
        6:  "NIFTY 500",
        14: "NIFTY500 MULTICAP 50:25:25",
    }

    @classmethod
    def get_index_constituents(cls, index_id: int = 1) -> list[str]:
        """
        Fetches the live stock list for a NIFTY index from NSE.

        NOTE: fetchStockCodes() has a stubConfigManager bug in the current
        PyPI release of PKNSETools. We use the Benny allIndices() call
        instead, which is confirmed working.

        index_id reference:
            1  → NIFTY 50
            2  → NIFTY NEXT 50
            4  → NIFTY 100
            5  → NIFTY 200
            6  → NIFTY 500

        Returns list of ticker symbols e.g. ["TCS", "RELIANCE", ...]
        Returns [] on failure — caller should fall back to hardcoded lists.
        """
        index_name = cls._INDEX_NAME_MAP.get(index_id, "NIFTY 50")

        try:
            from PKNSETools.Benny.NSE import NSE  # type: ignore

            with tempfile.TemporaryDirectory() as tmpdir:
                nse = NSE(download_folder=tmpdir)
                all_indices = nse.allIndices()

            # allIndices returns a dict with "data" key containing index info
            # We want the "indexSymbol" or similar field
            if not all_indices:
                return []

            # Find the matching index and extract its constituent symbols
            # The response structure: {"data": [{"index": "NIFTY 50", ...}, ...]}
            data = all_indices.get("data", [])
            for entry in data:
                if entry.get("index", "").upper() == index_name.upper():
                    # Some versions embed constituent list; otherwise empty
                    symbols = entry.get("constituents", [])
                    if symbols:
                        return [str(s).upper() for s in symbols]

            # allIndices gives index-level data, not constituent lists in this version.
            # Fall back: return empty so caller uses hardcoded NIFTY_50 list.
            print(f"[INFO] NSEAdapter: constituent list not in allIndices for '{index_name}'. "
                  f"Using hardcoded list in ingest scripts.")
            return []

        except ImportError:
            print("[WARN] PKNSETools not installed. Run: pip install PKNSETools")
            return []
        except Exception as e:
            print(f"[WARN] NSEAdapter.get_index_constituents failed (index={index_id}): {e}")
            return []

    # ──────────────────────────────────────────────
    # LATEST PRICE (simple scalar)
    # ──────────────────────────────────────────────

    @classmethod
    def get_latest_price(cls, ticker: str) -> float:
        """
        Returns just the LTP as a float using nseStockDataFetcher.getLatestPrice().
        The fastest / lowest-overhead way to get a current price from NSE.
        Returns 0.0 on failure.
        """
        try:
            from PKNSETools.PKNSEStockDataFetcher import nseStockDataFetcher  # type: ignore

            fetcher = nseStockDataFetcher()
            price = fetcher.getLatestPrice(ticker.upper())
            return float(price) if price else 0.0

        except ImportError:
            return 0.0
        except Exception as e:
            print(f"[WARN] NSEAdapter.get_latest_price failed for {ticker}: {e}")
            return 0.0

    # ──────────────────────────────────────────────
    # MARKET STATUS
    # ──────────────────────────────────────────────

    @classmethod
    def is_market_open(cls) -> bool:
        """
        Returns True if NSE equity market is currently open.
        Uses nseStockDataFetcher.capitalMarketStatus() for the official check.
        Falls back to an IST time-based heuristic if unavailable.
        """
        try:
            from PKNSETools.PKNSEStockDataFetcher import nseStockDataFetcher  # type: ignore

            fetcher = nseStockDataFetcher()
            status = fetcher.capitalMarketStatus()
            # capitalMarketStatus returns True/False or a status string
            if isinstance(status, bool):
                return status
            if isinstance(status, str):
                return "open" in status.lower()
            return cls._is_market_hours_ist()

        except ImportError:
            return cls._is_market_hours_ist()
        except Exception:
            return cls._is_market_hours_ist()

    @staticmethod
    def _is_market_hours_ist() -> bool:
        """Fallback heuristic: NSE is open Mon–Fri, 9:15–15:30 IST."""
        try:
            import pytz
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            if now.weekday() >= 5:
                return False
            open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
            close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
            return open_t <= now <= close_t
        except Exception:
            return False

    # ──────────────────────────────────────────────
    # INTRADAY SERIES (market hours only)
    # ──────────────────────────────────────────────

    @classmethod
    def get_intraday(cls, ticker: str) -> Dict[str, Any]:
        """
        Fetches real-time intraday price timestamps and LTP series.
        Only useful during market hours (9:15 AM – 3:30 PM IST).

        Uses PKNSETools.PKIntraDay.Intra_Day — confirmed class name from
        installed package inspection.
        """
        try:
            from PKNSETools.PKIntraDay import Intra_Day  # type: ignore

            intra = Intra_Day(ticker.upper())
            # intraDay() returns (timestamps_list, prices_list)
            result = intra.intraDay()
            if result and len(result) == 2:
                timestamps, prices = result
                return {
                    "ticker": ticker.upper(),
                    "timestamps": [str(t) for t in timestamps] if timestamps else [],
                    "prices": [float(p) for p in prices if p is not None] if prices else [],
                    "source": "nse_intraday",
                }
            return {}

        except ImportError:
            print("[WARN] PKNSETools not installed. Run: pip install PKNSETools")
            return {}
        except Exception as e:
            print(f"[WARN] NSEAdapter.get_intraday failed for {ticker}: {e}")
            return {}

    # ──────────────────────────────────────────────
    # CONVENIENCE: Fallback-aware LTP
    # ──────────────────────────────────────────────

    @classmethod
    def get_ltp_with_fallback(cls, ticker: str, yfinance_info: Optional[Dict] = None) -> float:
        """
        Returns the best available LTP using:
          1. nseStockDataFetcher.getLatestPrice()  (fastest NSE call)
          2. NSE Benny quote                        (richer but slower)
          3. yfinance info dict                     (if already fetched, pass it in)
          4. 0.0

        Call this from live_broker._from_nse() to avoid double-fetching.
        """
        # Try fast scalar first
        ltp = cls.get_latest_price(ticker)
        if ltp > 0:
            return ltp

        # Try full Benny quote
        quote = cls.get_quote(ticker)
        if quote.get("ltp", 0) > 0:
            return quote["ltp"]

        # Try yfinance info already in memory
        if yfinance_info:
            ltp = float(
                yfinance_info.get("currentPrice")
                or yfinance_info.get("regularMarketPrice")
                or yfinance_info.get("previousClose")
                or 0
            )
            if ltp > 0:
                return ltp

        print(f"[WARN] Could not resolve LTP for {ticker} from any NSE source.")
        return 0.0
