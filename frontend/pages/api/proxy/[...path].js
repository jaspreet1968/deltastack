/**
 * Server-side API proxy – forwards requests to FastAPI with X-API-Key.
 * The API key is NEVER exposed to the browser.
 *
 * Environment variables (set in systemd EnvironmentFile):
 *   DELTASTACK_API_KEY – the API key
 *   BACKEND_BASE_URL – e.g. https://api.deltastack.ai (or http://127.0.0.1:8000 for local)
 */
export default async function handler(req, res) {
  const apiKey = process.env.DELTASTACK_API_KEY;
  const apiBase = process.env.BACKEND_BASE_URL || 'http://127.0.0.1:8000';

  if (!apiKey) {
    return res.status(500).json({ error: 'DELTASTACK_API_KEY not configured on server' });
  }

  const path = req.query.path?.join('/') || '';
  const url = `${apiBase}/${path}`;

  try {
    const fetchOpts = {
      method: req.method,
      headers: {
        'X-API-Key': apiKey,
        'Content-Type': 'application/json',
      },
    };

    if (req.method !== 'GET' && req.method !== 'HEAD' && req.body) {
      fetchOpts.body = JSON.stringify(req.body);
    }

    const response = await fetch(url, fetchOpts);
    const contentType = response.headers.get('content-type') || '';

    if (contentType.includes('application/json')) {
      const data = await response.json();
      res.status(response.status).json(data);
    } else {
      const text = await response.text();
      res.status(response.status).send(text);
    }
  } catch (err) {
    console.error('API proxy error:', err.message);
    res.status(502).json({ error: 'API proxy error', detail: err.message });
  }
}
