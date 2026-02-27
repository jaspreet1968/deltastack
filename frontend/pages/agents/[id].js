import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import Link from 'next/link';

export default function AgentDashboard() {
  const router = useRouter();
  const { id } = router.query;
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    if (!id) return;
    fetch(`/api/proxy/agents/${id}/dashboard`)
      .then(r => r.json())
      .then(d => setData(d))
      .catch(e => setError(e.message));
  }, [id]);

  // Countdown timer
  useEffect(() => {
    const timer = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  if (error) return <div style={{ padding: 20 }}>Error: {error}</div>;
  if (!data) return <div style={{ padding: 20 }}>Loading...</div>;

  const agent = data.agent || {};
  const strategies = data.strategies || [];
  const runs = data.recent_runs || [];
  const signals = data.signals || [];
  const trades = data.trades || [];
  const errors = data.errors || [];

  // Find 0DTE strategy for Mad Max Today
  const dteStrat = strategies.find(s => s.strategy_name === '0dte_credit_spread');
  const params = typeof dteStrat?.params_json === 'object' ? dteStrat?.params_json : {};
  const underlying = params.underlying || 'QQQ';

  // Force exit countdown (15:45 ET)
  const etOffset = -5; // ET offset from UTC (simplified)
  const forceExitHour = 15;
  const forceExitMin = 45;
  const nowET = new Date(now.getTime() + (now.getTimezoneOffset() + etOffset * 60) * 60000);
  const exitTime = new Date(nowET);
  exitTime.setHours(forceExitHour, forceExitMin, 0, 0);
  const countdown = Math.max(0, Math.floor((exitTime - nowET) / 1000));
  const cdH = Math.floor(countdown / 3600);
  const cdM = Math.floor((countdown % 3600) / 60);
  const cdS = countdown % 60;

  return (
    <div style={{ maxWidth: 1100, margin: '0 auto', padding: 20, fontFamily: 'system-ui' }}>
      <Link href="/">← Back to Agents</Link>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 10 }}>
        <h1 style={{ margin: 0 }}>{agent.display_name || agent.name}</h1>
        <span style={{
          padding: '4px 12px', borderRadius: 12, fontSize: 12, fontWeight: 'bold',
          background: agent.risk_profile === 'SUPER_RISKY' ? '#ffcdd2' : '#c8e6c9',
          color: agent.risk_profile === 'SUPER_RISKY' ? '#b71c1c' : '#1b5e20',
        }}>{agent.risk_profile}</span>
      </div>
      <p style={{ color: '#666' }}>{agent.description}</p>

      {/* Mad Max Today Panel */}
      {dteStrat && (
        <div style={{ background: '#263238', color: '#fff', padding: 20, borderRadius: 8, marginBottom: 24 }}>
          <h3 style={{ margin: '0 0 12px', color: '#ff9800' }}>0DTE Today – {underlying}</h3>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12 }}>
            <div style={kpiBox}>
              <div style={{ fontSize: 11, opacity: 0.7 }}>Underlying</div>
              <div style={{ fontSize: 20, fontWeight: 'bold' }}>{underlying}</div>
            </div>
            <div style={kpiBox}>
              <div style={{ fontSize: 11, opacity: 0.7 }}>Max Trades/Day</div>
              <div style={{ fontSize: 20, fontWeight: 'bold' }}>5</div>
            </div>
            <div style={kpiBox}>
              <div style={{ fontSize: 11, opacity: 0.7 }}>Max Notional/Day</div>
              <div style={{ fontSize: 20, fontWeight: 'bold' }}>$20K</div>
            </div>
            <div style={kpiBox}>
              <div style={{ fontSize: 11, opacity: 0.7 }}>Max Loss/Day</div>
              <div style={{ fontSize: 20, fontWeight: 'bold' }}>$1.5K</div>
            </div>
            <div style={{ ...kpiBox, background: countdown > 0 ? '#e65100' : '#b71c1c' }}>
              <div style={{ fontSize: 11, opacity: 0.7 }}>Force Exit In</div>
              <div style={{ fontSize: 20, fontWeight: 'bold', fontFamily: 'monospace' }}>
                {countdown > 0 ? `${cdH}:${String(cdM).padStart(2,'0')}:${String(cdS).padStart(2,'0')}` : 'CLOSED'}
              </div>
            </div>
          </div>
          <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
            <button onClick={() => runTick()} style={btnStyle}>Run Tick (Plan Only)</button>
            <Link href={`/agents/${id}/replay`} style={{ ...btnStyle, background: '#546e7a', textDecoration: 'none' }}>
              Replay
            </Link>
          </div>
        </div>
      )}

      {/* KPI Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 12, marginBottom: 24 }}>
        <div style={{ padding: 14, background: '#e3f2fd', borderRadius: 6, textAlign: 'center' }}>
          <div style={{ fontSize: 11, color: '#666' }}>Strategies</div>
          <div style={{ fontSize: 22, fontWeight: 'bold' }}>{strategies.length}</div>
        </div>
        <div style={{ padding: 14, background: '#f3e5f5', borderRadius: 6, textAlign: 'center' }}>
          <div style={{ fontSize: 11, color: '#666' }}>Runs Today</div>
          <div style={{ fontSize: 22, fontWeight: 'bold' }}>{runs.length}</div>
        </div>
        <div style={{ padding: 14, background: '#fff3e0', borderRadius: 6, textAlign: 'center' }}>
          <div style={{ fontSize: 11, color: '#666' }}>Signals</div>
          <div style={{ fontSize: 22, fontWeight: 'bold' }}>{signals.length}</div>
        </div>
        <div style={{ padding: 14, background: errors.length > 0 ? '#ffebee' : '#e8f5e9', borderRadius: 6, textAlign: 'center' }}>
          <div style={{ fontSize: 11, color: '#666' }}>Errors</div>
          <div style={{ fontSize: 22, fontWeight: 'bold' }}>{errors.length}</div>
        </div>
      </div>

      {/* Strategies */}
      <h2>Strategies</h2>
      <table style={tableStyle}>
        <thead>
          <tr style={{ borderBottom: '2px solid #333' }}>
            <th style={thStyle}>Strategy</th>
            <th style={thStyle}>Mode</th>
            <th style={thStyle}>Enabled</th>
            <th style={thStyle}>Params</th>
          </tr>
        </thead>
        <tbody>
          {strategies.map(s => (
            <tr key={s.agent_strategy_id} style={{ borderBottom: '1px solid #eee' }}>
              <td style={tdStyle}>{s.strategy_name}</td>
              <td style={{ ...tdStyle, textAlign: 'center' }}>
                <span style={{
                  padding: '2px 8px', borderRadius: 8, fontSize: 11,
                  background: s.execution_mode === 'paper_live' ? '#c8e6c9' :
                              s.execution_mode === 'approved' ? '#bbdefb' : '#f5f5f5',
                }}>{s.execution_mode}</span>
              </td>
              <td style={{ ...tdStyle, textAlign: 'center' }}>{s.enabled ? '✅' : '❌'}</td>
              <td style={tdStyle}><code style={{ fontSize: 11 }}>{JSON.stringify(s.params_json)}</code></td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Recent Runs */}
      <h2>Recent Runs</h2>
      <table style={tableStyle}>
        <thead>
          <tr style={{ borderBottom: '2px solid #333' }}>
            <th style={thStyle}>Run ID</th><th style={thStyle}>Type</th>
            <th style={thStyle}>Status</th><th style={thStyle}>Started</th>
          </tr>
        </thead>
        <tbody>
          {runs.slice(0, 10).map(r => (
            <tr key={r.run_id} style={{ borderBottom: '1px solid #eee' }}>
              <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 11 }}>{r.run_id}</td>
              <td style={tdStyle}>{r.run_type}</td>
              <td style={tdStyle}>
                <span style={{ color: r.status === 'success' ? 'green' : r.status === 'failed' ? 'red' : '#666' }}>
                  {r.status}
                </span>
              </td>
              <td style={tdStyle}>{r.started_at}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Signals */}
      <h2>Recent Signals</h2>
      <table style={tableStyle}>
        <thead>
          <tr style={{ borderBottom: '2px solid #333' }}>
            <th style={thStyle}>Ticker</th><th style={thStyle}>Signal</th>
            <th style={thStyle}>Strategy</th><th style={thStyle}>As Of</th>
          </tr>
        </thead>
        <tbody>
          {signals.slice(0, 10).map((s, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #eee' }}>
              <td style={tdStyle}>{s.ticker}</td>
              <td style={{ ...tdStyle, fontWeight: 'bold',
                color: s.signal === 'BUY' ? '#2e7d32' : s.signal === 'SELL' ? '#c62828' : '#666' }}>
                {s.signal}
              </td>
              <td style={tdStyle}>{s.strategy}</td>
              <td style={tdStyle}>{s.as_of}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {trades.length > 0 && (
        <>
          <h2>Recent Trades</h2>
          <pre style={{ background: '#f5f5f5', padding: 12, borderRadius: 4, overflow: 'auto', maxHeight: 250, fontSize: 12 }}>
            {JSON.stringify(trades.slice(0, 10), null, 2)}
          </pre>
        </>
      )}
    </div>
  );

  async function runTick() {
    const today = new Date().toISOString().split('T')[0];
    const timeNow = String(nowET.getHours()).padStart(2, '0') + String(nowET.getMinutes()).padStart(2, '0');
    try {
      const r = await fetch(`/api/proxy/agents/${id}/tick`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: today, time: timeNow, mode: 'plan_only' }),
      });
      const d = await r.json();
      alert(`Tick result: ${d.decision || d.status || 'done'}\n${JSON.stringify(d, null, 2).slice(0, 500)}`);
    } catch (e) {
      alert('Tick failed: ' + e.message);
    }
  }
}

const kpiBox = { padding: 10, background: 'rgba(255,255,255,0.1)', borderRadius: 6, textAlign: 'center' };
const btnStyle = { padding: '8px 16px', background: '#ff9800', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 13 };
const tableStyle = { width: '100%', borderCollapse: 'collapse', marginBottom: 24 };
const thStyle = { textAlign: 'left', padding: 6 };
const tdStyle = { padding: 6 };
