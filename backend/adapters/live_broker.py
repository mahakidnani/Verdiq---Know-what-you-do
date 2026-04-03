import os
from typing import Dict, Any

class LiveBrokerAdapter:
    """
    Adapter for real-time market quotes.

    Priority fallback chain for getting LTP + 52-week range:
      1. Angel One SmartAPI  (real-time official exchange data — needs ANGEL_API_KEY)
      2. PKNSETools Benny    (NSE official API — free, no auth needed)
      3. yfinance            (free global fallback)
      4. Static mock         (last resort — keeps pipeline alive for UI dev)
    """

    @classmethod
    def get_realtime_quote(cls, exchange: str = "NSE", symbol_token: str = "RELIANCE") -> Dict[str, Any]:
        """
        Returns a quote dict with: ltp, 52WeekHigh, 52WeekLow, volume.
        Tries each source in priority order; returns the first successful result.
        """
        # ── Source 1: Angel One SmartAPI ──
        angel_key = os.getenv("ANGEL_API_KEY")
        if angel_key:
            quote = cls._from_angel(exchange, symbol_token)
            if quote.get("ltp", 0) > 0:
                return quote

        # ── Source 2: PKNSETools (NSE official API — free) ──
        quote = cls._from_nse(symbol_token)
        if quote.get("ltp", 0) > 0:
            return quote

        # ── Source 3: yfinance ──
        quote = cls._from_yfinance(symbol_token)
        if quote.get("ltp", 0) > 0:
            return quote

        # ── Source 4: Mock (UI dev mode) ──
        print(f"[WARN] All price sources failed for {symbol_token}. Using static mock.")
        return cls._get_mock_quote()

    # ──────────────────────────────────────────────
    # Source 1: Angel One SmartAPI
    # ──────────────────────────────────────────────

    @classmethod
    def _from_angel(cls, exchange: str, symbol_token: str) -> Dict[str, Any]:
        try:
            from SmartApi import SmartConnect  # type: ignore
            obj = SmartConnect(api_key=os.getenv("ANGEL_API_KEY"))
            obj.generateSession(
                os.getenv("ANGEL_CLIENT_ID", ""),
                os.getenv("ANGEL_PIN", "")
            )
            market_data = obj.marketStatus({"exchange": exchange, "tokens": [symbol_token]})
            if market_data.get("status") and "data" in market_data:
                d = market_data["data"]
                return {
                    "ltp": float(d.get("ltp", 0)),
                    "52WeekHigh": float(d.get("52WeekHigh", 0)),
                    "52WeekLow": float(d.get("52WeekLow", 0)),
                    "volume": int(d.get("volume", 0)),
                    "source": "angel_one",
                }
        except Exception as e:
            print(f"[WARN] Angel One failed for {symbol_token}: {e}")
        return {}

    # ──────────────────────────────────────────────
    # Source 2: PKNSETools (NSE official, free)
    # ──────────────────────────────────────────────

    @classmethod
    def _from_nse(cls, ticker: str) -> Dict[str, Any]:
        try:
            from backend.adapters.nse_adapter import NSEAdapter
            quote = NSEAdapter.get_quote(ticker)
            week = NSEAdapter.get_52week_range(ticker)
            if quote.get("ltp", 0) > 0:
                return {
                    "ltp": quote["ltp"],
                    "52WeekHigh": week.get("week_52_high", quote["ltp"]),
                    "52WeekLow": week.get("week_52_low", quote["ltp"]),
                    "volume": quote.get("volume", 0),
                    "source": "nse_official",
                }
        except Exception as e:
            print(f"[WARN] PKNSETools failed for {ticker}: {e}")
        return {}

    # ──────────────────────────────────────────────
    # Source 3: yfinance
    # ──────────────────────────────────────────────

    @classmethod
    def _from_yfinance(cls, ticker: str) -> Dict[str, Any]:
        try:
            import yfinance as yf
            ns = ticker if ticker.endswith(".NS") else f"{ticker}.NS"
            info = yf.Ticker(ns).info or {}
            ltp = float(
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or info.get("previousClose")
                or 0
            )
            if ltp > 0:
                return {
                    "ltp": ltp,
                    "52WeekHigh": float(info.get("fiftyTwoWeekHigh", ltp)),
                    "52WeekLow": float(info.get("fiftyTwoWeekLow", ltp)),
                    "volume": int(info.get("regularMarketVolume", 0)),
                    "source": "yfinance",
                }
        except Exception as e:
            print(f"[WARN] yfinance price fetch failed for {ticker}: {e}")
        return {}

    # ──────────────────────────────────────────────
    # Source 4: Static mock (keeps pipeline alive)
    # ──────────────────────────────────────────────

    @classmethod
    def _get_mock_quote(cls) -> Dict[str, Any]:
        return {
            "ltp": 2847.35,
            "52WeekHigh": 3024.90,
            "52WeekLow": 2210.50,
            "volume": 6804530,
            "source": "mock",
        }
