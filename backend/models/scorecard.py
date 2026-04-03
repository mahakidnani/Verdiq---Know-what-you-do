from pydantic import BaseModel, ConfigDict
from typing import List, Literal, Optional

class KPIMetric(BaseModel):
    label: str
    value: float
    unit: str
    trend: Literal["up", "down", "flat"]
    yoy_change: float
    history: List[float]
    years: List[str]
    interpretation: str
    health: Literal["good", "ok", "bad"]
    sector_median: Optional[float] = None
    health_note: Optional[str] = None

class ScorecardResponse(BaseModel):
    revenue: KPIMetric
    profit_margin: KPIMetric
    roe: KPIMetric
    debt_equity: KPIMetric
    pe_ratio: KPIMetric
    
    model_config = ConfigDict(extra="allow")
