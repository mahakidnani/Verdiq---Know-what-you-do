import { useState } from 'react'
import axios from 'axios'
import './App.css'

const API = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export default function App() {
  const [ticker, setTicker] = useState('')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [summary, setSummary] = useState(null)
  const [smartMoney, setSmartMoney] = useState(null)

  const scoreWeights = {
    revenue: 0.25,
    profit_margin: 0.2,
    roe: 0.2,
    debt_equity: 0.15,
    pe_ratio: 0.2,
  }

  const healthScore = {
    good: 100,
    ok: 60,
    bad: 20,
  }

  const calculateScore = (scorecard) => {
    if (!scorecard) return 0
    return Math.round(
      Object.entries(scoreWeights).reduce((total, [key, weight]) => {
        return total + (healthScore[scorecard[key]?.health] || 50) * weight
      }, 0) * 10,
    )
  }

  const verdictColor = (verdict) =>
    verdict === 'overvalued' ? 'var(--red)' : verdict === 'undervalued' ? 'var(--green)' : 'var(--yellow)'

  const trendArrow = (trend) => (trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→')

  const analyse = async () => {
    if (!ticker.trim()) return

    setLoading(true)
    setError(null)
    setData(null)
    setSummary(null)
    setSmartMoney(null)

    const symbol = ticker.trim().toUpperCase()

    try {
      const company = await axios.get(`${API}/api/v1/company/${symbol}`)
      setData(company.data)

      const summaryRes = await axios.get(`${API}/api/v1/company/${symbol}/summary`)
      setSummary(summaryRes.data.summary || summaryRes.data?.summary)

      const smartRes = await axios.get(`${API}/api/v1/company/${symbol}/smart-money`)
      setSmartMoney(smartRes.data)
    } catch (err) {
      setError('Could not fetch data. Check the ticker and try again.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="app-shell">
      <div className="hero-card">
        <div className="brand-row">
          <div className="brand-mark">V</div>
          <div>
            <h1>Verdiq</h1>
            <p>Clean stock intelligence for busy investors.</p>
          </div>
        </div>

        <div className="search-row">
          <input
            value={ticker}
            onChange={(e) => setTicker(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && analyse()}
            placeholder="Search ticker (INFY, TCS.NS, RELIANCE)"
            className="ticker-input"
          />
          <button onClick={analyse} disabled={loading} className="action-button">
            {loading ? 'Loading…' : 'Analyze'}
          </button>
        </div>

        <div className="hint">Use NSE tickers like INFY, TCS, RELIANCE for best results.</div>
      </div>

      {error && <div className="alert-box">{error}</div>}

      {data && (
        <main className="content-grid">
          <section className="card company-card">
            <div>
              <div className="eyebrow">Company</div>
              <h2>{data.company_name}</h2>
              <p>{data.sector}</p>
            </div>
            <div className="price-block">₹{data.last_price}</div>
          </section>

          <section className="card score-card">
            <div className="eyebrow">Verdiq Score</div>
            <div className="score-value">{calculateScore(data.scorecard)}</div>
            <div className="score-label">
              {calculateScore(data.scorecard) >= 700
                ? 'Strong'
                : calculateScore(data.scorecard) >= 400
                ? 'Average'
                : 'Needs Attention'}
            </div>
          </section>

          <section className="card verdict-card">
            <div className="eyebrow">Valuation Verdict</div>
            <div className="verdict-pill" style={{ backgroundColor: verdictColor(data.valuation.verdict) }}>
              {data.valuation.verdict.replace('_', ' ').toUpperCase()}
            </div>
            <p className="small-text">Confidence {data.valuation.confidence_score}%</p>
            <p className="rationale">{data.valuation.rationale}</p>
          </section>

          <section className="card metrics-grid">
            {Object.values(data.scorecard).map((metric) => (
              <div key={metric.label} className="metric-card">
                <div className="metric-title">{metric.label}</div>
                <div className="metric-value">
                  {metric.value}
                  <span className="metric-unit">{metric.unit}</span>
                </div>
                <div className="metric-note">{metric.interpretation}</div>
                <div className="metric-footnote">{trendArrow(metric.trend)} {metric.health}</div>
              </div>
            ))}
          </section>

          {summary && (
            <section className="card narrative-card">
              <div className="eyebrow">Quick summary</div>
              <p>{summary}</p>
            </section>
          )}

          {smartMoney && (
            <section className="card smart-money-card">
              <div className="eyebrow">Smart Money Tracker</div>
              <div className="smart-grid">
                {[
                  { label: 'Promoters', value: `${smartMoney.promoter_holding}%` },
                  { label: 'Institutions', value: `${smartMoney.fii_holding}%` },
                  { label: 'Retail', value: `${smartMoney.retail_holding}%` },
                ].map((item) => (
                  <div key={item.label} className="smart-card">
                    <div className="smart-value">{item.value}</div>
                    <div className="smart-label">{item.label}</div>
                  </div>
                ))}
              </div>
              <div className="signal-row">{smartMoney.signal_note}</div>
            </section>
          )}

          <section className="footnote-card">
            <p>Verdiq is for informational use only. Not investment advice.</p>
          </section>
        </main>
      )}
    </div>
  )
}