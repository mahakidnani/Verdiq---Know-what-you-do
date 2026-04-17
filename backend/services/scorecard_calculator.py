import yfinance as yf
from backend.models.scorecard import ScorecardResponse

class ScorecardCalculator:
    """
    Computes scorecard pillars based on yfinance data.
    """
    
    @classmethod
    def calculate_scorecard(cls, ticker: str, apify_data: dict = None) -> ScorecardResponse:
        stock = yf.Ticker(ticker)
        info = stock.info

        # Extract yfinance fields based on strict definitions
        profit_margin = info.get("profitMargins") or info.get("netMargins") or 0
        roe = info.get("returnOnEquity") or 0
        debt_to_equity = info.get("debtToEquity") or 0
        revenue = info.get("totalRevenue") or info.get("revenue") or 0
        revenue_growth = info.get("revenueGrowth") or info.get("earningsGrowth") or 0
        pe_ratio = info.get("trailingPE") or info.get("forwardPE") or 0
        current_ratio = info.get("currentRatio") or 0
        free_cashflow = info.get("freeCashflow") or 0
        net_income = info.get("netIncomeToCommon") or 0
        earnings_quality_score = 70 if free_cashflow > 0 else 40 if net_income > 0 else 20

        profit_margin_pct = profit_margin * 100
        roe_pct = roe * 100
        revenue_growth_pct = revenue_growth * 100

        # Pillar 1: Financial Health
        if current_ratio >= 2:
            financial_health = 85
        elif current_ratio >= 1.5:
            financial_health = 70
        elif current_ratio >= 1:
            financial_health = 55
        else:
            financial_health = 30

        # Pillar 2: Profitability
        if profit_margin_pct >= 20:
            profitability = 90
        elif profit_margin_pct >= 15:
            profitability = 75
        elif profit_margin_pct >= 8:
            profitability = 60
        elif profit_margin_pct >= 3:
            profitability = 40
        else:
            profitability = 20

        # Pillar 3: Valuation Fairness
        if pe_ratio <= 0:
            valuation_fairness = 50
        elif pe_ratio <= 15:
            valuation_fairness = 85
        elif pe_ratio <= 25:
            valuation_fairness = 65
        elif pe_ratio <= 35:
            valuation_fairness = 45
        else:
            valuation_fairness = 25

        # Pillar 4: Earnings Quality
        earnings_quality = earnings_quality_score

        # Pillar 5: Debt Safety
        if debt_to_equity <= 0:
            debt_safety = 90
        elif debt_to_equity <= 30:
            debt_safety = 85
        elif debt_to_equity <= 60:
            debt_safety = 70
        elif debt_to_equity <= 100:
            debt_safety = 50
        else:
            debt_safety = 25

        verdiq_score = round((financial_health * 0.25 + profitability * 0.20 + valuation_fairness * 0.20 + earnings_quality * 0.20 + debt_safety * 0.15) * 10)

        # Generate Tickertape-style dynamic rationale
        pillars = {
            "financial health": financial_health,
            "profitability": profitability,
            "valuation": valuation_fairness,
            "earnings quality": earnings_quality,
            "debt safety": debt_safety
        }
        
        strongest = max(pillars, key=pillars.get)
        weakest = min(pillars, key=pillars.get)
        
        if verdiq_score > 700:
            summary_rationale = f"This high score is primarily driven by exceptional {strongest}, though {weakest} represents a minor drag."
        elif verdiq_score < 400:
            summary_rationale = f"This low score is heavily dragged down by poor {weakest}, despite decent {strongest}."
        else:
            summary_rationale = f"A moderate score balanced by strong {strongest} but offset by underlying weakness in {weakest}."

        return ScorecardResponse(
            revenue=int(revenue),
            profit_margin=float(profit_margin_pct),
            net_profit_margin=float(profit_margin_pct),
            roe=float(roe_pct),
            debt_to_equity=float(debt_to_equity),
            pe_ratio=float(pe_ratio),
            current_ratio=float(current_ratio),
            free_cashflow=float(free_cashflow),
            revenue_growth=float(revenue_growth_pct),
            financial_health=financial_health,
            profitability=profitability,
            valuation_fairness=valuation_fairness,
            earnings_quality=earnings_quality,
            debt_safety=debt_safety,
            verdiq_score=verdiq_score,
            summary_rationale=summary_rationale
        )
