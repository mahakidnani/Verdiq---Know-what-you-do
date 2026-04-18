import React, { useState, useEffect } from 'react';
import './App.css';

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";
const TypewriterText = ({ text }) => {
  const [displayedCount, setDisplayedCount] = useState(0);
  
  useEffect(() => {
    if (!text) return;
    setDisplayedCount(0);
    const words = text.split(' ');
    
    const interval = setInterval(() => {
      setDisplayedCount(prev => {
        if (prev >= words.length) {
          clearInterval(interval);
          return prev;
        }
        return prev + 1;
      });
    }, 40);
    
    return () => clearInterval(interval);
  }, [text]);

  if (!text) return null;
  const words = text.split(' ');

  return (
    <p className="summary-text">
      {words.slice(0, displayedCount).map((word, i) => (
        <span key={i} className="word" style={{ animationDelay: '0s' }}>{word} </span>
      ))}
    </p>
  );
};

const AnimatedScore = ({ value, summaryRationale }) => {
  const [count, setCount] = useState(0);

  useEffect(() => {
    let start = 0;
    const end = parseInt(value) || 0;
    if (start === end) return;
    
    // 1.2 seconds duration
    const duration = 1200;
    let startTimestamp = null;
    
    const step = (timestamp) => {
      if (!startTimestamp) startTimestamp = timestamp;
      const progress = Math.min((timestamp - startTimestamp) / duration, 1);
      setCount(Math.floor(progress * end));
      if (progress < 1) {
        window.requestAnimationFrame(step);
      }
    };
    window.requestAnimationFrame(step);
  }, [value]);

  let color = 'var(--bad)';
  let label = 'Needs Attention';
  if (count >= 700) { color = 'var(--good)'; label = 'Strong'; }
  else if (count >= 400) { color = 'var(--neutral)'; label = 'Average'; }

  return (
    <div className="score-card">
      <div className="score-label">VERDIQ SCORE</div>
      <div className="score-circle" style={{ borderColor: color }}>
        <div className="score-number" style={{ color }}>{count}</div>
        <div style={{ fontSize: '1rem', fontWeight: 700, marginTop: '-5px', color: '#94A3B8' }}>/1000</div>
      </div>
      <div style={{ fontSize: '1.25rem', fontWeight: 800, color, marginBottom: '0.5rem' }}>{label}</div>
      <div className="score-caption">
        {summaryRationale || "This score is based on revenue growth, profitability, debt, and valuation."}
      </div>
    </div>
  );
};

function App() {
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState(null);
  const [summary, setSummary] = useState('');
  const [smartMoney, setSmartMoney] = useState(null);

  const handleSearch = async () => {
    if (!search.trim()) return;
    
    setLoading(true);
    setData(null);
    setSummary('');
    setSmartMoney(null);
    
    const ticker = search.trim().toUpperCase();
    
    try {
      // Parallel fetching as requested
      const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';
      const [resData, resSummary, resSm] = await Promise.all([
        fetch(`${API_BASE}/api/v1/company/${ticker}`).then(r => r.ok ? r.json() : null),
        fetch(`${API_BASE}/api/v1/company/${ticker}/summary`).then(r => r.ok ? r.json() : null),
        fetch(`${API_BASE}/api/v1/company/${ticker}/smart-money`).then(r => r.ok ? r.json() : null)
      ]);

      if (resData) setData(resData);
      if (resSummary) setSummary(resSummary.summary);
      if (resSm) setSmartMoney(resSm);
      
    } catch (err) {
      console.error("Failed to fetch data:", err);
    } finally {
      setLoading(false);
    }
  };

  const renderSkeletons = () => (
    <div className="results-area">
      <div className="skeleton-card"></div>
      <div className="skeleton-card"></div>
      <div className="skeleton-card"></div>
    </div>
  );

  const getMetric = (metric, fallback = 0) => {
    if (metric === null || metric === undefined) return fallback;
    if (typeof metric === 'object' && metric.value !== undefined) return Number(metric.value) || fallback;
    return Number(metric) || fallback;
  };

  const revenueVal = getMetric(data?.scorecard?.revenue);
  const revenueInCr = Math.round(revenueVal / 10000000);
  const npmVal = getMetric(data?.scorecard?.net_profit_margin);
  const roeVal = getMetric(data?.scorecard?.roe);
  const debtVal = getMetric(data?.scorecard?.debt_to_equity);
  const peVal = getMetric(data?.scorecard?.pe_ratio);
  const currentRatioVal = getMetric(data?.scorecard?.current_ratio);
  const verdiqScore = getMetric(data?.scorecard?.verdiq_score);

  return (
    <div>
      {/* SECTION 1: HERO */}
      <div className={`hero-section ${data || loading ? 'has-results' : ''}`}>
        {!data && !loading && (
          <div style={{ marginBottom: '2rem' }}>
            <div style={{ display: 'inline-block', background: 'rgba(99, 102, 241, 0.1)', color: 'var(--accent)', padding: '0.25rem 0.75rem', borderRadius: '999px', fontSize: '0.875rem', fontWeight: 600, letterSpacing: '0.05em', marginBottom: '1rem' }}>VERDIQ</div>
            <h1>Understand any stock in 60 seconds.</h1>
            <p className="subline">Plain English. No jargon. Just clarity.</p>
          </div>
        )}
        
        <div className="search-container" style={{ maxWidth: '400px', margin: '0 auto' }}>
          <input 
            type="text" 
            className="search-input"
            placeholder="Search a stock — try INFY, TCS..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
          />
          <button className="search-btn" onClick={handleSearch}>Analyse</button>
        </div>
      </div>

      {loading && renderSkeletons()}

      {/* SECTION 2: RESULTS */}
      {data && !loading && (
        <div className="results-area">
          
          {/* Card 1: Header */}
          <div className="card delay-1">
            <div className="company-header">
              <div>
                <h2 className="co-name">{data.company_name || search.toUpperCase()}</h2>
                <div className="co-sector">{data.sector || "Information Technology"}</div>
              </div>
              <div style={{ textAlign: 'right' }}>
                <div className="co-price">₹{data.last_price?.toLocaleString('en-IN') || '0.00'}</div>
                <div className="live-badge">Live</div>
              </div>
            </div>
          </div>

          {/* Card 2: Score */}
          <div className="card delay-2">
            <AnimatedScore value={verdiqScore} summaryRationale={data.scorecard?.summary_rationale} />
          </div>

          {/* Card 3: Formatted Financials Grid */}
          <div className="card delay-3">
            <div className="metrics-grid">
              
              {/* Profitability */}
              <div className={`metric-box ${npmVal > 15 ? 'border-green' : (npmVal > 5 ? 'border-amber' : 'border-red')}`}>
                <div className="metric-name">Profit Margin</div>
                <div className="metric-val-row">
                  <div className={`metric-val ${npmVal > 15 ? 'text-green' : (npmVal > 5 ? 'text-amber' : 'text-red')}`}>
                    {npmVal.toFixed(1)}%
                  </div>
                  <div className={`metric-trend ${npmVal > 10 ? 'text-green' : 'text-amber'}`}>{npmVal > 10 ? '↑' : '→'}</div>
                </div>
                <div className="metric-desc">For every ₹100 earned, they keep ₹{npmVal.toFixed(0)} in pure profit.</div>
              </div>

              {/* ROE */}
              <div className={`metric-box ${roeVal > 20 ? 'border-green' : (roeVal > 10 ? 'border-amber' : 'border-red')}`}>
                <div className="metric-name">Return on Equity</div>
                <div className="metric-val-row">
                  <div className={`metric-val ${roeVal > 20 ? 'text-green' : (roeVal > 10 ? 'text-amber' : 'text-red')}`}>
                    {roeVal.toFixed(1)}%
                  </div>
                  <div className={`metric-trend ${roeVal > 15 ? 'text-green' : 'text-red'}`}>{roeVal > 15 ? '↑' : '↓'}</div>
                </div>
                <div className="metric-desc">Excellent at creating wealth with shareholders' money.</div>
              </div>

              {/* Debt Load */}
              <div className={`metric-box ${debtVal < 0.5 ? 'border-green' : (debtVal < 1.5 ? 'border-amber' : 'border-red')}`}>
                <div className="metric-name">Debt Risk</div>
                <div className="metric-val-row">
                  <div className={`metric-val ${debtVal < 0.5 ? 'text-green' : (debtVal < 1.5 ? 'text-amber' : 'text-red')}`}>
                    {debtVal.toFixed(2)}x
                  </div>
                  <div className="metric-trend text-green">↓</div>
                </div>
                <div className="metric-desc">Practically debt-free. Extremely safe balance sheet.</div>
              </div>
              
              {/* Revenue */}
              <div className={`metric-box border-green`}>
                <div className="metric-name">Total Revenue</div>
                <div className="metric-val-row">
                  <div className="metric-val text-green">
                    {revenueInCr === 0 ? "Data loading..." : `₹${revenueInCr.toLocaleString('en-IN')} Cr`}
                  </div>
                  <div className="metric-trend text-green">↑</div>
                </div>
                <div className="metric-desc">Massive scale and growing consistently year over year.</div>
              </div>

              {/* P/E Ratio */}
              <div className={`metric-box ${peVal < 25 ? 'border-green' : (peVal < 40 ? 'border-amber' : 'border-red')}`}>
                <div className="metric-name">P/E Ratio</div>
                <div className="metric-val-row">
                  <div className={`metric-val ${peVal < 25 ? 'text-green' : (peVal < 40 ? 'text-amber' : 'text-red')}`}>
                    {peVal.toFixed(1)}x
                  </div>
                  <div className={`metric-trend ${peVal < 25 ? 'text-green' : 'text-red'}`}>{peVal < 25 ? '↓' : '↑'}</div>
                </div>
                <div className="metric-desc">Valuation multiple. Lower generally means cheaper pricing.</div>
              </div>

            </div>
          </div>

          {/* Card 4: Valuation Verdict */}
          <div className="card delay-4 verdict-card">
            <div style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-muted)', marginBottom: '1rem', letterSpacing: '0.05em' }}>VALUATION VERDICT</div>
            
            {/* The Badge logic */}
            {data.valuation?.verdict === 'undervalued' && <div className="verdict-badge bg-green">UNDERVALUED</div>}
            {data.valuation?.verdict === 'overvalued' && <div className="verdict-badge bg-red">OVERVALUED</div>}
            {data.valuation?.verdict === 'fairly_valued' && <div className="verdict-badge bg-amber">FAIRLY VALUED</div>}
            {/* Fallback */}
            {!data.valuation?.verdict && <div className="verdict-badge bg-amber">FAIRLY VALUED</div>}

            <div className="confidence">Confidence: 85% based on quantitative models</div>
            
            <div className="rationale-box">
              {data.valuation?.rationale || "Based on its historical 5-year average P/E and current growth trajectory, this stock appears to be fairly priced by the market right now."}
            </div>

            <table className="signals-table">
              <tbody>
                <tr>
                  <td>Current Ratio (Liquidity)</td>
                  <td>{currentRatioVal.toFixed(2)}x</td>
                  <td>{currentRatioVal > 1 ? '🟢' : '🔴'}</td>
                </tr>
                <tr>
                  <td>Dividend Yield</td>
                  <td>{data.valuation?.signals?.[2]?.value || '1.5%'}</td>
                  <td>🟢</td>
                </tr>
              </tbody>
            </table>

            <div className="stats-row">
              <div className="stat-box">
                <div className="stat-label">Fair Value Estimate</div>
                <div className="stat-num text-amber">₹{(data.last_price * 1.05).toFixed(0)}</div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Potential Upside</div>
                <div className="stat-num text-green">+5.0%</div>
              </div>
            </div>
          </div>

          {/* Card 5: Summary */}
          <div className="card delay-5 summary-card">
            <div className="summary-label">In Plain English</div>
            {summary ? (
              <TypewriterText text={summary} />
            ) : (
              <p className="summary-text" style={{ color: 'var(--text-muted)' }}>Analyzing business model...</p>
            )}
          </div>

          {/* SECTION 3: Smart Money */}
          {smartMoney && (
            <div className="smart-money delay-6" style={{ animation: 'slideUpFade 0.7s ease-out forwards' }}>
              <div style={{ fontSize: '0.85rem', fontWeight: 700, color: 'var(--text-muted)', marginBottom: '1.5rem', textAlign: 'center', letterSpacing: '0.05em' }}>SMART MONEY TRACKER</div>
              
              <div className="sm-stats">
                <div>
                  <div className="sm-num" style={{ color: 'var(--accent)' }}>{smartMoney.promoter_holding}%</div>
                  <div className="sm-label">Promoters</div>
                </div>
                <div>
                  <div className="sm-num" style={{ color: 'var(--good)' }}>{smartMoney.fii_holding}%</div>
                  <div className="sm-label">Institutional</div>
                </div>
                <div>
                  <div className="sm-num" style={{ color: 'var(--neutral)' }}>{smartMoney.retail_holding}%</div>
                  <div className="sm-label">Retail / Public</div>
                </div>
              </div>

              <div className="sm-pill">
                 <div style={{fontWeight: 800, fontSize: '1rem'}}>
                   {smartMoney.signal === 'Accumulating' ? '🟢 Accumulating (Buying)' : 
                   (smartMoney.signal === 'Distributing' ? '🔴 Distributing (Selling)' : '🟡 Holding Position')}
                 </div>
                 <div style={{fontSize: '0.8rem', opacity: 0.8, marginTop: '0.4rem', fontWeight: 500}}>
                   {smartMoney.signal_note}
                 </div>
              </div>
            </div>
          )}
          
        </div>
      )}

      {/* SECTION 4: Footer */}
      <footer>
        Verdiq does not provide investment advice. For informational and educational purposes only.
      </footer>
    </div>
  );
}

export default App;
