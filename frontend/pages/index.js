import { useEffect, useState } from 'react';
import Link from 'next/link';

export default function Home() {
  const [agents, setAgents] = useState([]);
  const [health, setHealth] = useState(null);
  const [dashboard, setDashboard] = useState(null);

  useEffect(() => {
    fetch('/api/proxy/agents').then(r => r.json()).then(d => setAgents(d.agents || [])).catch(() => {});
    fetch('/api/proxy/health').then(r => r.json()).then(d => setHealth(d)).catch(() => {});
    fetch('/api/proxy/dashboard/summary').then(r => r.json()).then(d => setDashboard(d)).catch(() => {});
  }, []);

  return (
    <div style={{ maxWidth: 1000, margin: '0 auto', padding: 20, fontFamily: 'system-ui' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1 style={{ margin: 0 }}>DeltaStack</h1>
        <span style={{ fontSize: 12, color: '#666' }}>v1.1.0 | Paper Only</span>
      </div>
      <p style={{ color: '#666' }}>Agent Platform for 0DTE Options & Equity Trading</p>

      {health && (
        <div style={{ padding: 12, background: health.status === 'ok' ? '#e8f5e9' : '#ffebee',
                      borderRadius: 6, marginBottom: 20 }}>
          Backend: <strong>{health.status}</strong> | Service: {health.service}
        </div>
      )}

      {dashboard && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 12, marginBottom: 24 }}>
          <div style={{ padding: 12, background: '#e3f2fd', borderRadius: 6, textAlign: 'center' }}>
            <div style={{ fontSize: 11, color: '#666' }}>Orders Today</div>
            <div style={{ fontSize: 24, fontWeight: 'bold' }}>{dashboard.orders_today || 0}</div>
          </div>
          <div style={{ padding: 12, background: '#f3e5f5', borderRadius: 6, textAlign: 'center' }}>
            <div style={{ fontSize: 11, color: '#666' }}>Broker</div>
            <div style={{ fontSize: 16, fontWeight: 'bold' }}>{dashboard.broker?.provider || 'paper'}</div>
          </div>
          <div style={{ padding: 12, background: '#fff3e0', borderRadius: 6, textAlign: 'center' }}>
            <div style={{ fontSize: 11, color: '#666' }}>Errors</div>
            <div style={{ fontSize: 24, fontWeight: 'bold' }}>{dashboard.recent_errors?.length || 0}</div>
          </div>
          <div style={{ padding: 12, background: '#e8f5e9', borderRadius: 6, textAlign: 'center' }}>
            <div style={{ fontSize: 11, color: '#666' }}>Agents</div>
            <div style={{ fontSize: 24, fontWeight: 'bold' }}>{agents.length}</div>
          </div>
        </div>
      )}

      <h2>Agents</h2>
      {agents.length === 0 ? (
        <p>No agents found. The Mad Max agent should be auto-seeded on startup.</p>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ borderBottom: '2px solid #333', textAlign: 'left' }}>
              <th style={{ padding: 8 }}>Agent</th>
              <th>Risk Profile</th>
              <th>Broker</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {agents.map(a => (
              <tr key={a.agent_id} style={{ borderBottom: '1px solid #eee' }}>
                <td style={{ padding: 8 }}>
                  <strong>{a.display_name || a.name}</strong>
                  <br /><small style={{ color: '#888' }}>{a.description}</small>
                </td>
                <td style={{
                  color: a.risk_profile === 'SUPER_RISKY' ? '#d32f2f' :
                         a.risk_profile === 'BALANCED' ? '#f57c00' : '#388e3c'
                }}>
                  {a.risk_profile}
                </td>
                <td>{a.broker_provider}</td>
                <td>{a.enabled ? '🟢 Active' : '🔴 Disabled'}</td>
                <td>
                  <Link href={`/agents/${a.agent_id}`}
                    style={{ padding: '4px 12px', background: '#1976d2', color: '#fff',
                             borderRadius: 4, textDecoration: 'none', fontSize: 13 }}>
                    Dashboard
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h2 style={{ marginTop: 30 }}>Quick Links</h2>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 8 }}>
        <a href="/api/proxy/health" target="_blank" style={linkStyle}>Health Check</a>
        <a href="/api/proxy/data/freshness" target="_blank" style={linkStyle}>Data Freshness</a>
        <a href="/api/proxy/broker/status" target="_blank" style={linkStyle}>Broker Status</a>
        <a href="/api/proxy/ops/status" target="_blank" style={linkStyle}>Ops Status</a>
        <a href="/api/proxy/metrics/basic" target="_blank" style={linkStyle}>Metrics</a>
        <a href="/api/proxy/stats/storage" target="_blank" style={linkStyle}>Storage Stats</a>
      </div>
    </div>
  );
}

const linkStyle = {
  padding: '8px 12px', background: '#f5f5f5', borderRadius: 4,
  textDecoration: 'none', color: '#333', fontSize: 13, textAlign: 'center',
};
