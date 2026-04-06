import { useState } from 'react'
import axios from 'axios'

const API = 'http://localhost:8000'

export default function App() {
  const [ticker, setTicker] = useState('')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [summary, setSummary] = useState(null)
  const [smartMoney, setSmartMoney] = useState(null)

  const analyse = async () => {
    if (!ticker) return
    setLoading(true)
    setError(null)
    try {
      const res = await axios.get(`${API}/api/v1/company/${ticker.toUpperCase()}`)
      setData(res.data)
      const sumRes = await axios.get(`${API}/api/v1/company/${ticker.toUpperCase()}/summary`)
      setSummary(sumRes.data.summary)

      const smRes = await axios.get(`${API}/api/v1/company/${ticker.toUpperCase()}/smart-money`)
setSmartMoney(smRes.data)
    } catch (e) {
      setError('Could not fetch data. Check the ticker and try again.')
    }
    setLoading(false)
  }

  const scoreFromHealths = (sc) => {
    const map = { good: 100, ok: 60, bad: 20 }
    const weights = { revenue: 0.25, profit_margin: 0.20, roe: 0.20, debt_equity: 0.15, pe_ratio: 0.20 }
    let score = 0
    for (const [key, weight] of Object.entries(weights)) {
      score += (map[sc[key]?.health] || 50) * weight
    }
    return Math.round(score * 10)
  }

  const verdictColor = (v) => v === 'overvalued' ? '#ef4444' : v === 'undervalued' ? '#22c55e' : '#f59e0b'
  const healthColor = (h) => h === 'good' ? '#22c55e' : h === 'ok' ? '#f59e0b' : '#ef4444'
  const trendArrow = (t) => t === 'up' ? '↑' : t === 'down' ? '↓' : '→'
  const bearingIcon = (b) => b === 'bullish' ? '🟢' : b === 'bearish' ? '🔴' : '🟡'

  return (
    <div style={{ fontFamily: 'system-ui', maxWidth: 900, margin: '0 auto', padding: 24, background: '#f8fafc', minHeight: '100vh' }}>

      {/* Header */}
      <div style={{ textAlign: 'center', marginBottom: 32 }}>
        <h1 style={{ fontSize: 36, fontWeight: 800, color: '#1e293b', margin: 0 }}>⚡ Verdiq</h1>
        <p style={{ color: '#64748b', marginTop: 8 }}>Stock research made simple for every investor</p>
      </div>

      {/* Search */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 32, justifyContent: 'center' }}>
        <input
          value={ticker}
          onChange={e => setTicker(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && analyse()}
          placeholder="Enter ticker (e.g. INFY, TCS.NS, AAPL)"
          style={{ padding: '12px 16px', fontSize: 16, border: '2px solid #e2e8f0', borderRadius: 12, width: 340, outline: 'none' }}
        />
        <button
          onClick={analyse}
          disabled={loading}
          style={{ padding: '12px 28px', background: '#6366f1', color: '#fff', border: 'none', borderRadius: 12, fontSize: 16, fontWeight: 700, cursor: 'pointer' }}
        >
          {loading ? 'Analysing...' : 'Analyse'}
        </button>
      </div>

      {error && <p style={{ color: '#ef4444', textAlign: 'center' }}>{error}</p>}

      {data && (
        <div>
          {/* Company Header */}
          <div style={{ background: '#fff', borderRadius: 16, padding: 24, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h2 style={{ margin: 0, color: '#1e293b' }}>{data.company_name}</h2>
            <p style={{ color: '#64748b', margin: '4px 0 0' }}>{data.sector} · ₹{data.last_price}</p>
          </div>

          {/* Feature 1: Verdiq Score */}
          <div style={{ background: '#fff', borderRadius: 16, padding: 24, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h3 style={{ margin: '0 0 16px', color: '#1e293b' }}>⚡ Verdiq Score</h3>
            {(() => {
              const score = scoreFromHealths(data.scorecard)
              const color = score >= 700 ? '#22c55e' : score >= 400 ? '#f59e0b' : '#ef4444'
              const label = score >= 700 ? 'Strong' : score >= 400 ? 'Average' : 'Needs Attention'
              return (
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: 72, fontWeight: 900, color }}>{score}</div>
                  <div style={{ fontSize: 18, color, fontWeight: 600 }}>{label}</div>
                  <div style={{ color: '#94a3b8', fontSize: 14 }}>out of 1000</div>
                </div>
              )
            })()}
          </div>

          {/* Feature 2: Visual Financial Scorecard */}
          <div style={{ background: '#fff', borderRadius: 16, padding: 24, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h3 style={{ margin: '0 0 16px', color: '#1e293b' }}>📊 Financial Scorecard</h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              {Object.values(data.scorecard).map((metric, i) => (
                <div key={i} style={{ border: `2px solid ${healthColor(metric.health)}`, borderRadius: 12, padding: 16 }}>
                  <div style={{ fontWeight: 700, color: '#1e293b', marginBottom: 4 }}>{metric.label}</div>
                  <div style={{ fontSize: 24, fontWeight: 800, color: healthColor(metric.health) }}>
                    {metric.value}{metric.unit} <span style={{ fontSize: 16 }}>{trendArrow(metric.trend)}</span>
                  </div>
                  <div style={{ fontSize: 13, color: '#64748b', marginTop: 4 }}>{metric.interpretation}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Feature 3: Valuation Verdict */}
          <div style={{ background: '#fff', borderRadius: 16, padding: 24, marginBottom: 20, boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
            <h3 style={{ margin: '0 0 16px', color: '#1e293b' }}>🎯 Valuation Verdict</h3>
            <div style={{ textAlign: 'center', marginBottom: 20 }}>
              <div style={{ display: 'inline-block', background: verdictColor(data.valuation.verdict), color: '#fff', padding: '10px 32px', borderRadius: 999, fontSize: 22, fontWeight: 800, letterSpacing: 1 }}>
                {data.valuation.verdict.replace('_', ' ').toUpperCase()}
              </div>
              <div style={{ color: '#64748b', marginTop: 8 }}>Confidence: {data.valuation.confidence} ({data.valuation.confidence_score}%)</div>
            </div>
            <p style={{ color: '#475569', lineHeight: 1.6, background: '#f8fafc', padding: 16, borderRadius: 12 }}>{data.valuation.rationale}</p>
            <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: 16 }}>
              <thead>
                <tr style={{ background: '#f1f5f9' }}>
                  <th style={{ padding: '10px 12px', textAlign: 'left', color: '#64748b', fontWeight: 600 }}>Signal</th>
                  <th style={{ padding: '10px 12px', textAlign: 'left', color: '#64748b', fontWeight: 600 }}>Value</th>
                  <th style={{ padding: '10px 12px', textAlign: 'left', color: '#64748b', fontWeight: 600 }}>Reading</th>
                </tr>
              </thead>
              <tbody>
                {data.valuation.signals.map((s, i) => (
                  <tr key={i} style={{ borderTop: '1px solid #e2e8f0' }}>
                    <td style={{ padding: '10px 12px', color: '#1e293b' }}>{s.label}</td>
                    <td style={{ padding: '10px 12px', fontWeight: 700 }}>{s.value}</td>
                    <td style={{ padding: '10px 12px' }}>{bearingIcon(s.bearing)} {s.note}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ marginTop: 16, display: 'flex', gap: 16, justifyContent: 'center' }}>
              <div style={{ textAlign: 'center', background: '#f0fdf4', padding: '12px 24px', borderRadius: 12 }}>
                <div style={{ fontSize: 12, color: '#64748b' }}>Fair Value</div>
                <div style={{ fontSize: 20, fontWeight: 800, color: '#22c55e' }}>₹{Math.round(data.valuation.fair_value_base)}</div>
              </div>
              <div style={{ textAlign: 'center', background: data.valuation.upside_pct > 0 ? '#f0fdf4' : '#fef2f2', padding: '12px 24px', borderRadius: 12 }}>
                <div style={{ fontSize: 12, color: '#64748b' }}>Upside</div>
                <div style={{ fontSize: 20, fontWeight: 800, color: data.valuation.upside_pct > 0 ? '#22c55e' : '#ef4444' }}>{data.valuation.upside_pct}%</div>
              </div>
            </div>
          </div>
           {/* Layman Business Breakdown */}
{summary && (
  <div style={{ background:'#fff', borderRadius:16, padding:24, marginBottom:20, boxShadow:'0 1px 4px rgba(0,0,0,0.08)' }}>
    <h3 style={{ margin:'0 0 12px', color:'#1e293b' }}>💬 What does this company actually do?</h3>
    <p style={{ color:'#475569', lineHeight:1.8, fontSize:16, margin:0 }}>{summary}</p>
  </div>
)}

{/* Smart Money Tracker */}
{smartMoney && (
  <div style={{ background:'#fff', borderRadius:16, padding:24, marginBottom:20, boxShadow:'0 1px 4px rgba(0,0,0,0.08)' }}>
    <h3 style={{ margin:'0 0 16px', color:'#1e293b' }}>🐋 Smart Money Tracker</h3>
    <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:12, marginBottom:16 }}>
      {[
        { label:'Promoters', value: smartMoney.promoter_holding + '%', color:'#6366f1' },
        { label:'Institutional (FII/DII)', value: smartMoney.fii_holding + '%', color:'#22c55e' },
        { label:'Retail', value: smartMoney.retail_holding + '%', color:'#f59e0b' }
      ].map((item, i) => (
        <div key={i} style={{ textAlign:'center', padding:16, background:'#f8fafc', borderRadius:12 }}>
          <div style={{ fontSize:24, fontWeight:800, color:item.color }}>{item.value}</div>
          <div style={{ fontSize:13, color:'#64748b', marginTop:4 }}>{item.label}</div>
        </div>
      ))}
    </div>
    <div style={{ background:'#f8fafc', padding:12, borderRadius:10, textAlign:'center' }}>
      <span style={{ fontWeight:700, color: smartMoney.signal === 'bullish' ? '#22c55e' : smartMoney.signal === 'bearish' ? '#ef4444' : '#f59e0b' }}>
        {smartMoney.signal === 'bullish' ? '🟢' : smartMoney.signal === 'bearish' ? '🔴' : '🟡'} {smartMoney.signal_note}
      </span>
    </div>
  </div>
)}
          <p style={{ textAlign: 'center', color: '#94a3b8', fontSize: 13 }}>Verdiq does not provide investment advice. For informational purposes only.</p>
        </div>
      )}
    </div>
  )
}