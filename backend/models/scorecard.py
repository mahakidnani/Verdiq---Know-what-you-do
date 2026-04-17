from pydantic import BaseModel, ConfigDict
from typing import Optional

class ScorecardResponse(BaseModel):
    revenue: float
    profit_margin: float
    net_profit_margin: float
    roe: float
    debt_to_equity: float
    pe_ratio: float
    current_ratio: float
    free_cashflow: float
    revenue_growth: float

    financial_health: int
    profitability: int
    valuation_fairness: int
    earnings_quality: int
    debt_safety: int
    verdiq_score: int
    summary_rationale: str
    
    model_config = ConfigDict(extra="allow")
