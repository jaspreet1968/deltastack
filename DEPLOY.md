# DeltaStack – Deployment Guide (v0.4.0)

## Pre-requisites (already done)

| Component | Status |
|-----------|--------|
| EC2 Amazon Linux 2023 | ✅ running |
| Python 3.11 + venv | ✅ `/home/ec2-user/apps/deltastack/venv` |
| Nginx reverse proxy | ✅ port 80/443 → 127.0.0.1:8000 |
| Let's Encrypt TLS | ✅ cert valid until 2026-05-07 |
| systemd unit | ✅ `deltastack-api.service` |

---

## 1. Copy code to EC2

```bash
rsync -avz --exclude='__pycache__' --exclude='.git' --exclude='venv' \
  ./ ec2-user@3.233.58.236:/home/ec2-user/apps/deltastack/
```

## 2. Install dependencies

```bash
ssh ec2-user@3.233.58.236
cd /home/ec2-user/apps/deltastack
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

**New in Phase C:** `duckdb` and `scipy` are now required.

## 3. Create and secure `.env`

```bash
cd /home/ec2-user/apps/deltastack
cp .env.example .env
nano .env
```

Fill in **real values**:
```
MASSIVE_API_KEY=<your-polygon-key>
DELTASTACK_API_KEY=<generate-a-strong-random-key>
DATA_DIR=/home/ec2-user/data/deltastack
LOG_LEVEL=INFO
TRADING_ENABLED=false
```

Generate a random API key:
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Lock permissions:
```bash
chmod 600 .env
```

## 4. Create data directories

```bash
mkdir -p /home/ec2-user/data/deltastack/{bars/day,metadata,options/snapshots}
```

> The DuckDB file (`deltastack.duckdb`) is auto-created on first startup in `DATA_DIR`.

## 5. Update systemd API service

Ensure the systemd unit has `EnvironmentFile`:

```bash
sudo nano /etc/systemd/system/deltastack-api.service
```

```ini
[Unit]
Description=DeltaStack API
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/apps/deltastack
EnvironmentFile=/home/ec2-user/apps/deltastack/.env
ExecStart=/home/ec2-user/apps/deltastack/venv/bin/uvicorn api.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Reload and restart:
```bash
sudo systemctl daemon-reload
sudo systemctl restart deltastack-api
sudo systemctl status deltastack-api --no-pager
```

## 6. Install nightly ingestion timer

```bash
sudo cp /home/ec2-user/apps/deltastack/systemd/deltastack-ingest.service /etc/systemd/system/
sudo cp /home/ec2-user/apps/deltastack/systemd/deltastack-ingest.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable deltastack-ingest.timer
sudo systemctl start deltastack-ingest.timer
sudo systemctl list-timers --all | grep deltastack
```

## 7. Validate endpoints

Replace `<KEY>` with your actual `DELTASTACK_API_KEY`.

### Health (public)
```bash
curl -s https://api.deltastack.ai/health
```

### Docs (public)
```bash
curl -s -o /dev/null -w "%{http_code}" https://api.deltastack.ai/docs
```

### Metrics (public)
```bash
curl -s https://api.deltastack.ai/metrics/basic
```

### Auth required (should 401 without key)
```bash
curl -s https://api.deltastack.ai/prices/AAPL
```

### Ingest daily bars
```bash
curl -s -X POST https://api.deltastack.ai/ingest/daily \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"ticker":"AAPL","start":"2024-01-01","end":"2026-01-01"}'
```

### Batch ingest
```bash
curl -s -X POST https://api.deltastack.ai/ingest/batch \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"tickers":["AAPL","MSFT"],"start":"2024-01-01","end":"2026-01-01"}'
```

### Get prices
```bash
curl -s "https://api.deltastack.ai/prices/AAPL?limit=5" -H "X-API-Key: <KEY>"
```

### Data status
```bash
curl -s https://api.deltastack.ai/data/status/AAPL -H "X-API-Key: <KEY>"
```

### SMA backtest
```bash
curl -s -X POST https://api.deltastack.ai/backtest/sma \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"ticker":"AAPL","start":"2024-01-01","end":"2025-12-31","fast":10,"slow":30}'
```

### Buy-and-hold backtest
```bash
curl -s -X POST https://api.deltastack.ai/backtest/buy_hold \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"ticker":"AAPL","start":"2024-01-01","end":"2025-12-31"}'
```

### Portfolio SMA backtest (new in Phase C)
```bash
curl -s -X POST https://api.deltastack.ai/backtest/portfolio_sma \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{
    "tickers":["AAPL","MSFT","NVDA"],
    "start":"2024-01-01","end":"2025-12-31",
    "fast":10,"slow":30,
    "initial_cash":100000,"max_positions":3,
    "risk_per_trade":0.02,"commission_per_trade":1.0,"slippage_bps":2
  }'
```

### SMA signal
```bash
curl -s -X POST https://api.deltastack.ai/signals/sma \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"ticker":"AAPL","fast":10,"slow":30}'
```

### Options chain snapshot (new in Phase C)
```bash
curl -s -X POST https://api.deltastack.ai/options/chain/snapshot \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"underlying":"SPY","as_of":"2026-02-06"}'
```

### Options chain retrieval
```bash
curl -s "https://api.deltastack.ai/options/chain/SPY?as_of=2026-02-06&type=call&limit=10" \
  -H "X-API-Key: <KEY>"
```

### Compute Greeks
```bash
curl -s -X POST https://api.deltastack.ai/options/greeks \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"spot":450,"strike":460,"tte_years":0.08,"iv":0.25,"option_type":"call"}'
```

### Paper trading (TRADING_ENABLED=false → 503)
```bash
# Should return 503 when TRADING_ENABLED=false
curl -s -X POST https://api.deltastack.ai/trade/order \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"ticker":"AAPL","side":"BUY","qty":10}'

# To enable, set TRADING_ENABLED=true in .env and restart:
# sudo systemctl restart deltastack-api

# Then:
curl -s -X POST https://api.deltastack.ai/trade/order \
  -H "Content-Type: application/json" -H "X-API-Key: <KEY>" \
  -d '{"ticker":"AAPL","side":"BUY","qty":10}'

curl -s https://api.deltastack.ai/trade/positions -H "X-API-Key: <KEY>"
curl -s https://api.deltastack.ai/trade/account -H "X-API-Key: <KEY>"
```

### Ingestion status (Phase D)
```bash
curl -s https://api.deltastack.ai/ingest/status -H "X-API-Key: <KEY>"
```

### Storage stats (Phase D)
```bash
curl -s https://api.deltastack.ai/stats/storage -H "X-API-Key: <KEY>"
```

### Risk status (Phase D – requires TRADING_ENABLED=true)
```bash
curl -s https://api.deltastack.ai/trade/risk -H "X-API-Key: <KEY>"
```

## 8. Install backup timer (Phase D)

```bash
sudo cp /home/ec2-user/apps/deltastack/systemd/deltastack-backup.service /etc/systemd/system/
sudo cp /home/ec2-user/apps/deltastack/systemd/deltastack-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now deltastack-backup.timer
sudo systemctl list-timers --all | grep deltastack
```

## 9. Run tests

```bash
cd /home/ec2-user/apps/deltastack
source venv/bin/activate
bash scripts/test.sh
```

## 10. Troubleshooting

```bash
# API logs
sudo journalctl -u deltastack-api -n 200 --no-pager

# Nightly ingest logs
sudo journalctl -u deltastack-ingest -n 200 --no-pager

# Nginx check
sudo nginx -t && sudo systemctl reload nginx

# TLS check
curl -v https://api.deltastack.ai/health

# DuckDB file check
ls -la /home/ec2-user/data/deltastack/deltastack.duckdb
```

---

## File structure

```
/home/ec2-user/apps/deltastack/
├── api/
│   ├── __init__.py
│   ├── main.py                    # FastAPI app v0.3.0 + lifespan
│   ├── middleware.py              # Auth + rate limiting + observability
│   └── routers/
│       ├── __init__.py
│       ├── ingest.py             # POST /ingest/daily, /batch, /universe
│       ├── prices.py             # GET  /prices/{ticker}
│       ├── backtest.py           # POST /backtest/sma, /buy_hold, /portfolio_sma
│       ├── data_status.py        # GET  /data/status/{ticker}
│       ├── signals.py            # POST /signals/sma
│       ├── options.py            # POST /options/chain/snapshot, GET /options/chain/{u}, POST /options/greeks
│       ├── trade.py              # POST /trade/order, GET /trade/positions, GET /trade/account
│       └── metrics.py            # GET  /metrics/basic
├── deltastack/
│   ├── __init__.py
│   ├── __main__.py               # CLI entry-point
│   ├── config.py                 # Pydantic settings
│   ├── data/
│   │   ├── __init__.py
│   │   ├── storage.py            # Parquet read/write
│   │   └── validation.py         # Data quality checks
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── polygon.py            # Polygon daily bars client
│   │   └── options_chain.py      # Polygon options chain ingestion
│   ├── backtest/
│   │   ├── __init__.py
│   │   ├── base.py               # Strategy ABC
│   │   ├── sma.py                # SMA crossover engine
│   │   ├── buy_hold.py           # Buy-and-hold benchmark
│   │   └── portfolio_sma.py      # Multi-ticker portfolio SMA
│   ├── options/
│   │   ├── __init__.py
│   │   └── greeks.py             # Black-Scholes greeks + IV solver
│   ├── broker/
│   │   ├── __init__.py
│   │   ├── base.py               # Broker ABC
│   │   └── paper.py              # Paper broker (simulated fills)
│   └── db/
│       ├── __init__.py
│       ├── connection.py          # DuckDB connection + auto-migrations
│       └── dao.py                 # Data access objects
├── config/
│   └── universe.txt              # Default ticker universe
├── systemd/
│   ├── deltastack-ingest.service
│   └── deltastack-ingest.timer
├── .env                          # secrets (chmod 600, NOT in git)
├── .env.example
├── .gitignore
├── requirements.txt
└── DEPLOY.md
```

## API Endpoints Summary (v0.3.0)

| Method | Path | Auth | Rate Limited | Description |
|--------|------|------|--------------|-------------|
| GET | `/health` | No | No | Health check |
| GET | `/docs` | No | No | Swagger UI |
| GET | `/redoc` | No | No | ReDoc |
| GET | `/metrics/basic` | No | No | Uptime + request counters |
| POST | `/ingest/daily` | Yes | 30 RPM | Ingest single ticker |
| POST | `/ingest/batch` | Yes | 30 RPM | Ingest multiple tickers |
| POST | `/ingest/universe` | Yes | 30 RPM | Ingest from universe file |
| GET | `/prices/{ticker}` | Yes | No | Get stored daily bars |
| GET | `/data/status/{ticker}` | Yes | No | Metadata/coverage info |
| POST | `/backtest/sma` | Yes | 60 RPM | SMA crossover backtest |
| POST | `/backtest/buy_hold` | Yes | 60 RPM | Buy-and-hold benchmark |
| POST | `/backtest/portfolio_sma` | Yes | 60 RPM | Multi-ticker portfolio backtest |
| POST | `/signals/sma` | Yes | No | Latest SMA signal |
| POST | `/options/chain/snapshot` | Yes | 30 RPM | Ingest options chain |
| GET | `/options/chain/{underlying}` | Yes | No | Retrieve stored chain |
| POST | `/options/greeks` | Yes | 30 RPM | Compute BS greeks |
| POST | `/trade/order` | Yes | 30 RPM | Paper trade (kill switch) |
| GET | `/trade/positions` | Yes | No | Paper positions |
| GET | `/trade/account` | Yes | No | Paper account summary |

### Kill Switch

`TRADING_ENABLED` defaults to `false`. All `/trade/*` endpoints return **503** until you explicitly set `TRADING_ENABLED=true` in `.env` and restart the service. This is a safety measure – no real money is ever at risk.
