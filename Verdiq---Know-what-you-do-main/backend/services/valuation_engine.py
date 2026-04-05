import statistics
from backend.models.valuation import ValuationVerdictResponse, ValuationSignal
from backend.adapters.live_broker import LiveBrokerAdapter
from backend.adapters.apify_screener import ApifyScreenerAdapter
from backend.adapters.llm_client import LLMClientAdapter

class ValuationEngine:
    """
    Computes a 3-Point Valuation Verdict based on a rigid logic tree incorporating real-time pricing
    and 5-year averages, finally triggering cheap/free LLMs for a concise rationale.
    """
    
    @classmethod
    async def generate_valuation(cls, ticker: str) -> ValuationVerdictResponse:
        """
        Executes the three-signal tree:
        1. P/E vs 5yr historical average
        2. P/E vs sector median
        3. Price vs 52-week average
        """
        
        # 1. Gather all required cross-layer data
        quote = LiveBrokerAdapter.get_realtime_quote(symbol_token=ticker)
        apify_funcs = ApifyScreenerAdapter.get_deep_fundamentals(ticker)
        
        current_price = float(quote.get("ltp", 0))
        high_52 = float(quote.get("52WeekHigh", current_price))
        low_52 = float(quote.get("52WeekLow", current_price))
        avg_52 = (high_52 + low_52) / 2
        
        financials = apify_funcs.get("financials", {})
        pe_history = financials.get("PE", {}).get("history", [])
        
        # Ensure we have data
        current_pe = pe_history[-1] if pe_history else 0
        avg_pe = statistics.mean(pe_history) if len(pe_history) >= 2 else current_pe
        sector_pe = financials.get("SectorPE", avg_pe)

        # 2. Extract specific signals based on rule engine
        signals = []
        bear_count = 0
        bull_count = 0
        
        # SIGNAL A: Historical P/E Discount/Premium
        if current_pe > avg_pe * 1.15:
            bearing = "bearish"
            note = f"Trading at {((current_pe/avg_pe)-1)*100:.0f}% premium to 5-yr avg of {avg_pe:.1f}x"
            bear_count += 1
        elif current_pe < avg_pe * 0.85:
            bearing = "bullish"
            note = f"Trading at {((1-(current_pe/avg_pe)))*100:.0f}% discount to 5-yr avg"
            bull_count += 1
        else:
            bearing = "neutral"
            note = "In-line with 5-yr historical average"
            
        signals.append(ValuationSignal(label="Current P/E vs History", value=f"{current_pe:.1f}x", note=note, weight=40, bearing=bearing))

        # SIGNAL B: Sector Premium / Discount
        if current_pe > sector_pe * 1.1:
            bearing_sec = "bearish"
            note_sec = f"Valued at {((current_pe/sector_pe)-1)*100:.0f}% premium to sector peers"
            bear_count += 1
        elif current_pe < sector_pe * 0.9:
            bearing_sec = "bullish"
            note_sec = f"Valued cheaper than sector median of {sector_pe:.1f}x"
            bull_count += 1
        else:
            bearing_sec = "neutral"
            note_sec = "Valued roughly inline with industry peers"

        signals.append(ValuationSignal(label="P/E vs Sector Median", value=f"{sector_pe:.1f}x", note=note_sec, weight=35, bearing=bearing_sec))
            
        # SIGNAL C: 52-Week Mean Reversion
        if current_price > avg_52 * 1.15:
            bearing_px = "bearish"
            note_px = "Strong run — limited mean-revert room (extended above 52w avg)"
            bear_count += 1
        elif current_price < avg_52 * 0.85:
            bearing_px = "bullish"
            note_px = "Crushed below 52w avg — highly oversold"
            bull_count += 1
        else:
            bearing_px = "neutral"
            note_px = "Trading in the middle of 52-week consolidated range"
            
        signals.append(ValuationSignal(label="Price vs 52-Week Avg", value=f"₹{avg_52:.0f}", note=note_px, weight=25, bearing=bearing_px))

        # 3. Decision Logic Evaluation & Confidence Scoring
        if bear_count >= 2:
            verdict = "overvalued"
        elif bull_count >= 2:
            verdict = "undervalued"
        else:
            verdict = "fairly_valued"
            
        # Confidence Score
        max_flags = max(bear_count, bull_count)
        if max_flags == 3:
            confidence = "High"
            confidence_score = 90
        elif max_flags == 2:
            confidence = "Medium"
            confidence_score = 65
        else:
            confidence = "Low"
            confidence_score = 40
            
        # Fair value basic multipliers (simplified for MVP)
        eps = current_price / current_pe if current_pe > 0 else 0
        base = eps * avg_pe
        bear = eps * (avg_pe * 0.8)
        bull = eps * (avg_pe * 1.2)
        
        # 4. Trigger the LLM for the Rationale
        rationale = await LLMClientAdapter.generate_valuation_rationale(
            ticker=ticker,
            signals=[s.model_dump() for s in signals],
            current_pe=current_pe,
            avg_pe=avg_pe,
            verdict=verdict
        )

        return ValuationVerdictResponse(
            verdict=verdict,
            confidence=confidence,
            confidence_score=confidence_score,
            rationale=rationale,
            signals=signals,
            fair_value_bear=bear,
            fair_value_base=base,
            fair_value_bull=bull,
            current_price=current_price,
            upside_pct=round(((base / current_price) - 1) * 100, 1) if current_price else 0.0
        )
