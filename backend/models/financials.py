"""
Pydantic models for normalized financial data.

Both the Apify and yfinance pipelines normalize raw data into these models
before writing to Supabase. This guarantees a single, clean contract
regardless of data source.
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal


class NormalizedYearFinancials(BaseModel):
    """One row in the historical_financials table — a single fiscal year for one company."""

    ticker: str
    fiscal_year: str                                      # "FY25", "FY24", etc.

    # P&L
    total_revenue: Optional[float] = None                 # ₹ Crores
    net_profit: Optional[float] = None                    # ₹ Crores
    operating_profit: Optional[float] = None              # EBITDA / Operating Profit ₹ Cr
    net_profit_margin: Optional[float] = None             # %

    # Balance Sheet
    total_equity: Optional[float] = None                  # ₹ Crores
    total_debt: Optional[float] = None                    # ₹ Crores
    debt_to_equity: Optional[float] = None                # ratio (x)

    # Ratios
    roe: Optional[float] = None                           # %
    roce: Optional[float] = None                          # %
    pe_ratio: Optional[float] = None                      # x
    eps: Optional[float] = None                           # ₹

    source: Literal["apify", "yfinance"] = "apify"


class NormalizedCompanyFundamentals(BaseModel):
    """
    The complete output of a single company ingestion.
    Contains both the aggregated metrics (for financial_metrics_cache)
    and the year-by-year rows (for historical_financials).
    """

    ticker: str
    company_name: Optional[str] = None
    sector: Optional[str] = None

    # Aggregated current-year metrics (goes into financial_metrics_cache)
    current_pe: Optional[float] = None
    historical_pe_5yr: Optional[float] = None
    sector_pe: Optional[float] = None
    roe_percentage: Optional[float] = None
    debt_to_equity: Optional[float] = None
    revenue_growth_3yr: Optional[float] = None
    net_profit_margin: Optional[float] = None

    # History arrays (stored as JSONB in financial_metrics_cache)
    pe_history: list[float] = Field(default_factory=list)
    roe_history: list[float] = Field(default_factory=list)
    de_history: list[float] = Field(default_factory=list)
    revenue_history: list[float] = Field(default_factory=list)
    profit_margin_history: list[float] = Field(default_factory=list)
    history_years: list[str] = Field(default_factory=list)

    # Year-by-year rows (goes into historical_financials)
    yearly_financials: list[NormalizedYearFinancials] = Field(default_factory=list)

    source: Literal["apify", "yfinance"] = "apify"
