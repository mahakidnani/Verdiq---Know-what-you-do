import os
import textwrap
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic

class LLMClientAdapter:
    """
    Adapter bridging token-efficient AI generation. Uses GPT-4o-mini or Claude 3 Haiku/Sonnet
    optimised specifically for extremely low token footprint.
    """

    @classmethod
    async def generate_valuation_rationale(cls, ticker: str, signals: list[dict], current_pe: float, avg_pe: float, verdict: str) -> str:
        """
        Uses only free/freemium API keys loaded into .env to return a exactly 2-sentence rationale.
        """
        prompt = textwrap.dedent(f"""
            Role: Financial Analyst. Provide exactly 2 sentences justifying why {ticker} is {verdict}.
            Data: Current P/E={current_pe}x. 5-Yr Avg P/E={avg_pe}x. Signals: {signals}.
            Tone: Objective, institutional, zero jargon.
            Constraint: EXACTLY two sentences. Focus on the P/E and one key momentum metric.
        """).strip()

        # Try Anthropic first, fallback to OpenAI if API key not present
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        openai_key = os.getenv("OPENAI_API_KEY")

        try:
            if anthropic_key:
                client = AsyncAnthropic(api_key=anthropic_key)
                response = await client.messages.create(
                    model="claude-3-haiku-20240307",  # Ultra low cost tier
                    max_tokens=80,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text.strip()
            
            elif openai_key:
                client = AsyncOpenAI(api_key=openai_key)
                response = await client.chat.completions.create(
                    model="gpt-4o-mini", # Free/extremely low cost tier
                    max_tokens=80,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.choices[0].message.content.strip()

            print(f"[WARN] No LLM keys found. Using deterministic fallback rationale for {ticker}.")
            # Fallback if no API key
            return (
                f"{ticker} is currently trading at {current_pe}x P/E, which deviates significantly from its 5-year historical average of {avg_pe}x. "
                f"Given the recent momentum signals, the market appears to be highly pricing in its growth narrative, suggesting a '{verdict}' stance."
            )
        
        except Exception as e:
            print(f"[ERROR] LLM generation failed: {e}")
            return "Unable to generate real-time AI rationale at this moment due to connection or rate-limiting issues."
        
        
    @classmethod
    async def generate(cls, prompt: str, max_tokens: int = 250) -> str:
        """
        General-purpose LLM text generation for any prompt.
        Used by: Layman Business Breakdown, future features.
        """
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        openai_key = os.getenv("OPENAI_API_KEY")

        try:
            if anthropic_key:
                client = AsyncAnthropic(api_key=anthropic_key)
                response = await client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text.strip()

            elif openai_key:
                client = AsyncOpenAI(api_key=openai_key)
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.choices[0].message.content.strip()

            return "AI summary unavailable — no API key configured."

        except Exception as e:
            print(f"[ERROR] LLM generate failed: {e}")
            return "Unable to generate summary at this moment."
