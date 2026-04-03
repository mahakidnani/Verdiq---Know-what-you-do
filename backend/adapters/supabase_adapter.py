import os
import statistics
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client
from typing import Dict, Any, Optional, List

from backend.models.financials import NormalizedCompanyFundamentals

# Cache staleness threshold — data older than this triggers a fresh API fetch
CACHE_TTL_HOURS = 24


class SupabaseAdapter:
    """
    Production-grade read-through cache adapter for the 3-table VERDIQ schema:
      1. companies              — static company profile (rarely changes)
      2. financial_metrics_cache — heavy numbers from Apify/yfinance (24hr TTL)
      3. verdiq_intelligence     — expensive LLM outputs & verdict (24hr TTL)

    Security model:
      - READS  use the anon key (public SELECT via RLS policy)
      - WRITES use the service_role key (bypasses RLS entirely)
    """

    # ──────────────────────────────────────────────
    # Client Initialisation (Dual-Key Pattern)
    # ──────────────────────────────────────────────

    @classmethod
    def _get_read_client(cls) -> Optional[Client]:
        """Returns a Supabase client using the anon key (read-only via RLS)."""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            print("[WARN] SUPABASE_URL or SUPABASE_KEY not set. DB caching disabled.")
            return None
        return create_client(url, key)

    @classmethod
    def _get_write_client(cls) -> Optional[Client]:
        """Returns a Supabase client using the service_role key (bypasses RLS for writes)."""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        if not url or not key:
            # Graceful fallback: try the anon key (will fail if RLS blocks writes)
            anon_key = os.getenv("SUPABASE_KEY")
            if not url or not anon_key:
                print("[WARN] No Supabase keys found. DB caching disabled.")
                return None
            print("[WARN] SUPABASE_SERVICE_KEY not set — falling back to anon key for writes.")
            print("       Writes will FAIL if RLS is enabled. Set SUPABASE_SERVICE_KEY in .env.")
            return create_client(url, anon_key)
        return create_client(url, key)

    # ──────────────────────────────────────────────
    # READ: Check if fresh cached data exists
    # ──────────────────────────────────────────────

    @classmethod
    def _is_fresh(cls, last_updated_str: Optional[str]) -> bool:
        """Returns True if the record was updated within the last CACHE_TTL_HOURS."""
        if not last_updated_str:
            return False
        try:
            last_updated = datetime.fromisoformat(last_updated_str.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - last_updated
            return age < timedelta(hours=CACHE_TTL_HOURS)
        except Exception:
            return False

    @classmethod
    def get_cached_analysis(cls, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Attempts to read all 3 tables for a ticker.
        Returns a fully assembled response dict if ALL data is fresh (< 24hrs old).
        Returns None on cache miss or stale data — triggering the slow API path.
        """
        client = cls._get_read_client()
        if not client:
            return None

        try:
            # 1. Check company exists
            company = client.table("companies").select("*").eq("ticker", ticker).execute()
            if not company.data:
                print(f"[CACHE] MISS — {ticker} not in companies table.")
                return None

            # 2. Check financial metrics freshness
            metrics = client.table("financial_metrics_cache").select("*").eq("ticker", ticker).execute()
            if not metrics.data or not cls._is_fresh(metrics.data[0].get("last_updated")):
                print(f"[CACHE] MISS — {ticker} metrics stale or missing.")
                return None

            # 3. Check intelligence freshness
            intel = client.table("verdiq_intelligence").select("*").eq("ticker", ticker).execute()
            if not intel.data or not cls._is_fresh(intel.data[0].get("last_updated")):
                print(f"[CACHE] MISS — {ticker} intelligence stale or missing.")
                return None

            # All 3 tables are fresh — assemble the full API response
            c = company.data[0]
            m = metrics.data[0]
            i = intel.data[0]

            print(f"[CACHE] HIT — Serving {ticker} from Supabase (< {CACHE_TTL_HOURS}hrs old).")

            return cls._assemble_response(c, m, i)

        except Exception as e:
            print(f"[ERROR] Supabase read failed: {e}")
            return None

    @classmethod
    def is_cache_fresh(cls, ticker: str) -> bool:
        """
        Lightweight check: does fresh cached data exist for this ticker?
        Single query per table — doesn't pull all columns.
        Useful for health dashboards and pre-warming decisions.
        """
        client = cls._get_read_client()
        if not client:
            return False

        try:
            metrics = (
                client.table("financial_metrics_cache")
                .select("last_updated")
                .eq("ticker", ticker)
                .execute()
            )
            if not metrics.data or not cls._is_fresh(metrics.data[0].get("last_updated")):
                return False

            intel = (
                client.table("verdiq_intelligence")
                .select("last_updated")
                .eq("ticker", ticker)
                .execute()
            )
            if not intel.data or not cls._is_fresh(intel.data[0].get("last_updated")):
                return False

            return True
        except Exception:
            return False

    @classmethod
    def get_company_list(cls) -> List[Dict[str, Any]]:
        """
        Returns all active companies in the database.
        Powers search autocomplete and pre-warming scripts.
        """
        client = cls._get_read_client()
        if not client:
            return []

        try:
            result = (
                client.table("companies")
                .select("ticker, company_name, sector")
                .eq("is_active", True)
                .order("company_name")
                .execute()
            )
            return result.data or []
        except Exception as e:
            print(f"[ERROR] Failed to fetch company list: {e}")
            return []

    @classmethod
    def _assemble_response(cls, company: dict, metrics: dict, intel: dict) -> Dict[str, Any]:
        """Reconstructs the exact JSON shape the frontend expects from the 3 DB rows."""

        years = metrics.get("history_years", [])

        # Helper to build a KPI block from stored history arrays
        def build_kpi(label, history_key, unit, value_key=None, invert=False, sector_median=None):
            history = metrics.get(history_key, [])
            current = history[-1] if history else 0
            previous = history[-2] if len(history) >= 2 else current
            yoy = round(current - previous, 2)
            trend = "up" if yoy > 0 else ("down" if yoy < 0 else "flat")

            if label == "Return on Equity":
                health = "good" if current > 15 else ("ok" if current > 8 else "bad")
            elif label == "Debt-to-Equity":
                health = "good" if current < 0.5 else ("bad" if current > 1.0 else "ok")
            elif label == "P/E vs Sector":
                s = sector_median or 0
                health = "bad" if current > s * 1.5 else ("ok" if current > s else "good")
            elif invert:
                health = "good" if yoy < 0 else ("bad" if yoy > 0 else "ok")
            else:
                health = "good" if yoy > 0 else ("bad" if yoy < 0 else "ok")

            return {
                "label": label, "value": current, "unit": unit,
                "trend": trend, "yoy_change": yoy,
                "history": history, "years": years,
                "interpretation": "",  # Frontend can generate or we can store
                "health": health,
                "sector_median": sector_median,
                "health_note": "Lower is better" if invert else None
            }

        scorecard = {
            "revenue": build_kpi("Revenue", "revenue_history", "₹ Cr"),
            "profit_margin": build_kpi("Net Profit Margin", "profit_margin_history", "%"),
            "roe": build_kpi("Return on Equity", "roe_history", "%"),
            "debt_equity": build_kpi("Debt-to-Equity", "de_history", "x", invert=True),
            "pe_ratio": build_kpi("P/E vs Sector", "pe_history", "x", sector_median=metrics.get("sector_pe")),
        }

        valuation = {
            "verdict": intel.get("valuation_verdict", "fairly_valued"),
            "confidence": intel.get("valuation_confidence", "Low"),
            "confidence_score": intel.get("valuation_confidence_score", 40),
            "rationale": intel.get("valuation_rationale", ""),
            "signals": intel.get("valuation_signals", []),
            "fair_value_bear": intel.get("fair_value_bear", 0),
            "fair_value_base": intel.get("fair_value_base", 0),
            "fair_value_bull": intel.get("fair_value_bull", 0),
            "current_price": metrics.get("current_price", 0),
            "upside_pct": intel.get("upside_pct", 0),
        }

        return {
            "ticker": company["ticker"],
            "company_name": company["company_name"],
            "sector": company["sector"],
            "last_price": metrics.get("current_price", 0),
            "last_updated": metrics.get("last_updated", ""),
            "scorecard": scorecard,
            "valuation": valuation,
            "layman_summary": intel.get("layman_summary"),
        }

    # ──────────────────────────────────────────────
    # WRITE: Push fresh analysis into the 3 tables
    # ──────────────────────────────────────────────

    @classmethod
    def cache_full_analysis(
        cls,
        ticker: str,
        company_name: str,
        sector: str,
        scorecard_data: Dict[str, Any],
        valuation_data: Dict[str, Any],
        apify_financials: Dict[str, Any],
        layman_summary: Optional[str] = None,
    ) -> bool:
        """
        Upserts data across all 3 tables after a fresh API fetch.
        Called on the SLOW PATH only.
        Uses service_role key to bypass RLS for write operations.

        Note: `last_updated` is set automatically by a Postgres trigger —
        no need to pass it manually in the payload.
        """
        client = cls._get_write_client()
        if not client:
            return False

        financials = apify_financials.get("financials", {})

        try:
            # ── Table 1: companies (upsert static profile) ──
            client.table("companies").upsert({
                "ticker": ticker,
                "company_name": company_name,
                "sector": sector,
                "is_active": True,
            }).execute()

            # ── Table 2: financial_metrics_cache (upsert heavy numbers) ──
            pe_history = financials.get("PE", {}).get("history", [])
            roe_history = financials.get("ROE", {}).get("history", [])
            de_history = financials.get("DebtToEquity", {}).get("history", [])
            rev_history = financials.get("Revenue_Cr", {}).get("history", [])
            pm_history = financials.get("NetProfitMargin", {}).get("history", [])

            avg_pe = round(statistics.mean(pe_history), 2) if len(pe_history) >= 2 else (pe_history[-1] if pe_history else 0)

            # Revenue growth over last 3 years
            rev_growth_3yr = 0
            if len(rev_history) >= 4:
                rev_growth_3yr = round(((rev_history[-1] / rev_history[-4]) - 1) * 100, 2)

            client.table("financial_metrics_cache").upsert({
                "ticker": ticker,
                "current_pe": pe_history[-1] if pe_history else None,
                "historical_pe_5yr": avg_pe,
                "sector_pe": financials.get("SectorPE"),
                "roe_percentage": roe_history[-1] if roe_history else None,
                "debt_to_equity": de_history[-1] if de_history else None,
                "revenue_growth_3yr": rev_growth_3yr,
                "net_profit_margin": pm_history[-1] if pm_history else None,
                "current_price": valuation_data.get("current_price"),
                "week_52_high": None,  # Populated when Angel One is connected
                "week_52_low": None,
                "revenue_history": rev_history,
                "profit_margin_history": pm_history,
                "roe_history": roe_history,
                "de_history": de_history,
                "pe_history": pe_history,
                "history_years": apify_financials.get("years", []),
                # last_updated is set automatically by Postgres trigger
            }).execute()

            # ── Table 3: verdiq_intelligence (upsert expensive outputs) ──
            client.table("verdiq_intelligence").upsert({
                "ticker": ticker,
                "verdiq_score_total": None,  # Phase 2
                "score_breakdown": {},       # Phase 2
                "valuation_verdict": valuation_data.get("verdict"),
                "valuation_confidence": valuation_data.get("confidence"),
                "valuation_confidence_score": valuation_data.get("confidence_score"),
                "valuation_rationale": valuation_data.get("rationale"),
                "valuation_signals": valuation_data.get("signals", []),
                "fair_value_bear": valuation_data.get("fair_value_bear"),
                "fair_value_base": valuation_data.get("fair_value_base"),
                "fair_value_bull": valuation_data.get("fair_value_bull"),
                "upside_pct": valuation_data.get("upside_pct"),
                "layman_summary": layman_summary,  # Passed from LLM when available
                # last_updated is set automatically by Postgres trigger
            }).execute()

            print(f"[CACHE] WRITE — Successfully cached {ticker} across all 3 tables.")
            return True

        except Exception as e:
            print(f"[ERROR] Supabase write failed for {ticker}: {e}")
            return False

    # ──────────────────────────────────────────────
    # WRITE: historical_financials (batch ingestion)
    # ──────────────────────────────────────────────

    @classmethod
    def write_historical_financials(
        cls,
        fundamentals: NormalizedCompanyFundamentals,
    ) -> bool:
        """
        Bulk-upserts year-by-year financial rows into historical_financials.
        Called by the batch ingestion scripts (not the live API endpoint).

        Also upserts the company profile and aggregated metrics cache
        so all 4 tables stay in sync from a single ingestion run.
        """
        client = cls._get_write_client()
        if not client:
            return False

        ticker = fundamentals.ticker

        try:
            # ── 1. Upsert company profile ──
            if fundamentals.company_name:
                client.table("companies").upsert({
                    "ticker": ticker,
                    "company_name": fundamentals.company_name,
                    "sector": fundamentals.sector or "Unknown",
                    "is_active": True,
                }).execute()

            # ── 2. Bulk-upsert year-by-year rows into historical_financials ──
            rows = [
                {
                    "ticker": row.ticker,
                    "fiscal_year": row.fiscal_year,
                    "total_revenue": row.total_revenue,
                    "net_profit": row.net_profit,
                    "operating_profit": row.operating_profit,
                    "net_profit_margin": row.net_profit_margin,
                    "total_equity": row.total_equity,
                    "total_debt": row.total_debt,
                    "debt_to_equity": row.debt_to_equity,
                    "roe": row.roe,
                    "roce": row.roce,
                    "pe_ratio": row.pe_ratio,
                    "eps": row.eps,
                    "source": row.source,
                }
                for row in fundamentals.yearly_financials
            ]

            if rows:
                client.table("historical_financials").upsert(rows).execute()
                print(f"[BATCH] Wrote {len(rows)} historical rows for {ticker}.")

            # ── 3. Upsert aggregated metrics cache (for live API reads) ──
            client.table("financial_metrics_cache").upsert({
                "ticker": ticker,
                "current_pe": fundamentals.current_pe,
                "historical_pe_5yr": fundamentals.historical_pe_5yr,
                "sector_pe": fundamentals.sector_pe,
                "roe_percentage": fundamentals.roe_percentage,
                "debt_to_equity": fundamentals.debt_to_equity,
                "revenue_growth_3yr": fundamentals.revenue_growth_3yr,
                "net_profit_margin": fundamentals.net_profit_margin,
                "pe_history": fundamentals.pe_history,
                "roe_history": fundamentals.roe_history,
                "de_history": fundamentals.de_history,
                "revenue_history": fundamentals.revenue_history,
                "profit_margin_history": fundamentals.profit_margin_history,
                "history_years": fundamentals.history_years,
                # last_updated auto-set by Postgres trigger
            }).execute()

            return True

        except Exception as e:
            print(f"[ERROR] Batch write failed for {ticker}: {e}")
            return False

    # ──────────────────────────────────────────────
    # PARSE: Extract VERDIQ pillars from raw Screener JSON
    # ──────────────────────────────────────────────

    @staticmethod
    def parse_verdiq_pillars(apify_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extracts the 5 core financial pillars from the raw Apify/Screener JSON
        and packages them cleanly for a Supabase upsert into financial_metrics_cache.

        Keys expected in apify_json:
          - 'Ratios'          : dict of scalar ratios
          - 'Profit_and_loss' : list of yearly dicts with 'Sales', 'Net Profit', etc.

        Returns a dict ready to hand to the scoring algorithm or upsert directly.
        """
        if not apify_json or 'Ratios' not in apify_json:
            return {"error": "Invalid JSON payload — 'Ratios' key missing."}

        ratios = apify_json.get('Ratios', {})
        pnl = apify_json.get('Profit_and_loss', [])

        def safe(val) -> float:
            try:
                if isinstance(val, str):
                    val = val.replace('%', '').replace(',', '').strip()
                return float(val) if val is not None else 0.0
            except (ValueError, TypeError):
                return 0.0

        # Pillar 1: Valuation — P/E ratio
        current_pe = safe(ratios.get('Stock P/E', ratios.get('PE', 0)))

        # Pillar 2 & 3: Profitability & Health — ROCE / Debt
        roe = safe(ratios.get('ROE %', ratios.get('ROCE %', 0)))
        debt_to_equity = safe(ratios.get('Debt to equity', ratios.get('Debt_to_equity', 0)))

        # Pillar 4: Financial Trend — 3-year Revenue CAGR
        revenue_growth_3yr = 0.0
        if len(pnl) >= 4:
            current_revenue = safe(pnl[-1].get('Sales', pnl[-1].get('Revenue', 0)))
            revenue_3yr_ago = safe(pnl[-4].get('Sales', pnl[-4].get('Revenue', 0)))
            if revenue_3yr_ago > 0:
                revenue_growth_3yr = round(
                    ((current_revenue - revenue_3yr_ago) / revenue_3yr_ago) * 100, 2
                )

        # Pillar 5: Profit Quality — Net Profit Margin
        net_profit_margin = 0.0
        if pnl:
            last = pnl[-1]
            sales = safe(last.get('Sales', last.get('Revenue', 0)))
            profit = safe(last.get('Net Profit', last.get('Net_profit', 0)))
            if sales > 0:
                net_profit_margin = round((profit / sales) * 100, 2)

        # Map company name back to NSE ticker (caller provides ticker)
        return {
            "current_pe": current_pe,
            "roe_percentage": roe,
            "debt_to_equity": debt_to_equity,
            "revenue_growth_3yr": revenue_growth_3yr,
            "net_profit_margin": net_profit_margin,
        }
