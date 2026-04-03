from pydantic import BaseModel
from typing import List, Literal

class ValuationSignal(BaseModel):
    label: str
    value: str
    note: str
    weight: int
    bearing: Literal["bearish", "bullish", "neutral"]

class ValuationVerdictResponse(BaseModel):
    verdict: Literal["overvalued", "fairly_valued", "undervalued"]
    confidence: Literal["High", "Medium", "Low"]
    confidence_score: int
    rationale: str
    signals: List[ValuationSignal]
    fair_value_bear: float
    fair_value_base: float
    fair_value_bull: float
    current_price: float
    upside_pct: float
