from typing import List, Dict, Any
from backend.models.scorecard import ScorecardResponse, KPIMetric
from backend.adapters.apify_screener import ApifyScreenerAdapter
from backend.adapters.yfinance_adapter import YFinanceAdapter

class ScorecardCalculator:
    """
    Computes 3-to-5 year trends, assigns health indicators (`good/ok/bad`),
    and generates plain-English interpretation for each KPI.
    """
    
    @classmethod
    def analyze_trend(cls, history: List[float], invert_health: bool = False) -> tuple[str, float, str]:
        """
        Calculates YoY change and relative directional trend.
        If `invert_health` is True (e.g. Debt/Equity), going 'down' is 'good'.
        """
        if len(history) < 2:
            return "flat", 0.0, "ok"

        current = history[-1]
        previous = history[-2]
        yoy = round(current - previous, 2)
        
        if yoy > 0:
            trend = "up"
            health = "bad" if invert_health else "good"
        elif yoy < 0:
            trend = "down"
            health = "good" if invert_health else "bad"
        else:
            trend = "flat"
            health = "ok"
            
        return trend, yoy, health
    
    @classmethod
    def calculate_scorecard(cls, ticker: str) -> ScorecardResponse:
        """
        Orchestrates extracting fundamentals and transforming them into precisely matching UI expected schema.
        """
        # We rely heavily on Screener fundamentals (via Apify) for institutional accuracy
        data = ApifyScreenerAdapter.get_deep_fundamentals(ticker)
        financials = data.get("financials", {})
        years = data.get("years", [])

        # REVENUE
        rev_history = financials.get("Revenue_Cr", {}).get("history", [])
        rtrend, ryoy, rhealth = cls.analyze_trend(rev_history)
        revenue_kpi = KPIMetric(
            label="Revenue",
            value=rev_history[-1] if rev_history else 0,
            unit="₹ Cr",
            trend=rtrend,
            yoy_change=ryoy,
            history=rev_history,
            years=years,
            interpretation="Consistently growing top line" if rhealth == "good" else "Revenue showing friction",
            health=rhealth
        )

        # PROFIT MARGIN
        pm_history = financials.get("NetProfitMargin", {}).get("history", [])
        pmtrend, pmyoy, pmhealth = cls.analyze_trend(pm_history)
        pm_kpi = KPIMetric(
            label="Net Profit Margin",
            value=pm_history[-1] if pm_history else 0,
            unit="%",
            trend=pmtrend,
            yoy_change=pmyoy,
            history=pm_history,
            years=years,
            interpretation="Margins expanding steadily" if pmhealth == "good" else "Squeezed margins this year",
            health=pmhealth
        )

        # ROE
        roe_history = financials.get("ROE", {}).get("history", [])
        rtrend2, ryoy2, _ = cls.analyze_trend(roe_history)
        current_roe = roe_history[-1] if roe_history else 0
        roe_health = "good" if current_roe > 15 else ("ok" if current_roe > 8 else "bad")
        roe_kpi = KPIMetric(
            label="Return on Equity",
            value=current_roe,
            unit="%",
            trend=rtrend2,
            yoy_change=ryoy2,
            history=roe_history,
            years=years,
            interpretation="Strong returns for shareholders" if roe_health == "good" else "Returns require monitoring",
            health=roe_health
        )

        # DEBT-TO-EQUITY
        de_history = financials.get("DebtToEquity", {}).get("history", [])
        dtrend, dyoy, dhealth = cls.analyze_trend(de_history, invert_health=True)
        # Exception override for absolute levels
        current_de = de_history[-1] if de_history else 0
        if current_de < 0.5: dhealth = "good"
        elif current_de > 1.0: dhealth = "bad"
        de_kpi = KPIMetric(
            label="Debt-to-Equity",
            value=current_de,
            unit="x",
            trend=dtrend,
            yoy_change=dyoy,
            history=de_history,
            years=years,
            interpretation="Debt load reducing year-on-year" if dtrend=="down" else "Debt is increasing",
            health=dhealth,
            health_note="Lower is better"
        )

        # P/E vs SECTOR
        pe_history = financials.get("PE", {}).get("history", [])
        ptrend, pyoy, _ = cls.analyze_trend(pe_history, invert_health=True)
        current_pe = pe_history[-1] if pe_history else 0
        sector_pe = financials.get("SectorPE", 0)
        phealth = "bad" if current_pe > sector_pe * 1.5 else ("ok" if current_pe > sector_pe else "good")
        pe_kpi = KPIMetric(
            label="P/E vs Sector",
            value=current_pe,
            unit="x",
            trend=ptrend,
            yoy_change=pyoy,
            history=pe_history,
            years=years,
            interpretation="Premium to sector — growth priced in" if current_pe > sector_pe else "Trading at a discount to sector peers",
            health=phealth,
            sector_median=sector_pe
        )

        return ScorecardResponse(
            revenue=revenue_kpi,
            profit_margin=pm_kpi,
            roe=roe_kpi,
            debt_equity=de_kpi,
            pe_ratio=pe_kpi
        )
