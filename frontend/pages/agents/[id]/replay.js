import { useRouter } from 'next/router';
import { useState } from 'react';
import Link from 'next/link';

export default function ReplayPage() {
  const router = useRouter();
  const { id } = router.query;
  const [replayDate, setReplayDate] = useState(new Date().toISOString().split('T')[0]);
  const [startTime, setStartTime] = useState('1000');
  const [endTime, setEndTime] = useState('1415');
  const [interval, setInterval_] = useState(5);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  async function runReplay() {
    setLoading(true);
    try {
      const r = await fetch(`/api/proxy/agents/${id}/replay`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date: replayDate,
          start_time: startTime,
          end_time: endTime,
          interval_minutes: interval,
        }),
      });
      const d = await r.json();
      setResult(d);
    } catch (e) {
      setResult({ error: e.message });
    }
    setLoading(false);
  }

  return (
    <div style={{ maxWidth: 1100, margin: '0 auto', padding: 20, fontFamily: 'system-ui' }}>
      <Link href={`/agents/${id}`}>← Back to Agent Dashboard</Link>
      <h1>Replay Mode</h1>
      <p style={{ color: '#666' }}>Step through a day's snapshots to debug agent decisions.</p>

      <div style={{ display: 'flex', gap: 12, alignItems: 'flex-end', marginBottom: 24, flexWrap: 'wrap' }}>
        <div>
          <label style={labelStyle}>Date</label>
          <input type="date" value={replayDate} onChange={e => setReplayDate(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Start Time (HHMM)</label>
          <input value={startTime} onChange={e => setStartTime(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>End Time (HHMM)</label>
          <input value={endTime} onChange={e => setEndTime(e.target.value)} style={inputStyle} />
        </div>
        <div>
          <label style={labelStyle}>Interval (min)</label>
          <input type="number" value={interval} onChange={e => setInterval_(Number(e.target.value))} style={inputStyle} />
        </div>
        <button onClick={runReplay} disabled={loading}
          style={{ padding: '8px 20px', background: '#1976d2', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', height: 36 }}>
          {loading ? 'Running...' : 'Run Replay'}
        </button>
      </div>

      {result && result.error && (
        <div style={{ padding: 12, background: '#ffebee', borderRadius: 6, marginBottom: 16 }}>
          Error: {result.error || result.detail}
        </div>
      )}

      {result && result.timeline && (
        <>
          <div style={{ marginBottom: 16, color: '#666' }}>
            Replay ID: <code>{result.replay_id}</code> | Ticks: {result.ticks_evaluated} | Underlying: {result.underlying}
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid #333', textAlign: 'left' }}>
                <th style={{ padding: 6 }}>Time</th>
                <th>Decision</th>
                <th>Signal</th>
                <th>Short Strike</th>
                <th>Long Strike</th>
                <th>Credit</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {result.timeline.map((t, i) => (
                <tr key={i} style={{
                  borderBottom: '1px solid #eee',
                  background: t.decision === 'BUY' ? '#e8f5e9' : t.decision === 'error' ? '#ffebee' : 'transparent',
                }}>
                  <td style={{ padding: 6, fontFamily: 'monospace' }}>{t.time}</td>
                  <td style={{ fontWeight: 'bold',
                    color: t.decision === 'BUY' ? '#2e7d32' : t.decision === 'skip' ? '#666' : '#c62828' }}>
                    {t.decision}
                  </td>
                  <td>{t.signal || '-'}</td>
                  <td>{t.short_strike || '-'}</td>
                  <td>{t.long_strike || '-'}</td>
                  <td>{t.credit ? `$${t.credit}` : '-'}</td>
                  <td style={{ fontSize: 12, color: '#888' }}>{t.reason || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

const labelStyle = { display: 'block', fontSize: 11, color: '#666', marginBottom: 2 };
const inputStyle = { padding: '6px 10px', border: '1px solid #ddd', borderRadius: 4, fontSize: 14 };
