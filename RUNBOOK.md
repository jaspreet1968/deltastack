# DeltaStack Operational Runbook

## Quick Reference

| Service | Command |
|---------|---------|
| API status | `sudo systemctl status deltastack-api --no-pager` |
| API logs | `sudo journalctl -u deltastack-api -n 200 --no-pager` |
| API restart | `sudo systemctl restart deltastack-api` |
| Ingest logs | `sudo journalctl -u deltastack-ingest -n 200 --no-pager` |
| Backup logs | `sudo journalctl -u deltastack-backup -n 50 --no-pager` |
| Nginx test | `sudo nginx -t` |
| Timer list | `sudo systemctl list-timers --all \| grep deltastack` |

---

## Common Issues and Fixes

### 1. API not responding (502 Bad Gateway from Nginx)

**Symptom:** `curl https://api.deltastack.ai/health` returns 502.

**Diagnosis:**
```bash
sudo systemctl status deltastack-api --no-pager
sudo journalctl -u deltastack-api -n 100 --no-pager
```

**Fixes:**
- If service is stopped: `sudo systemctl restart deltastack-api`
- If crashing on startup: check for missing `.env` variables or broken imports
- If port conflict: `ss -tlnp | grep 8000`

### 2. SSL certificate renewal

**Symptom:** Browser shows certificate expired warning.

```bash
# Check cert expiry
sudo certbot certificates

# Renew
sudo certbot renew

# If automatic renewal failed
sudo certbot renew --force-renewal
sudo systemctl reload nginx
```

**Prevention:** Verify auto-renewal cron:
```bash
sudo certbot renew --dry-run
```

### 3. Nginx issues

```bash
# Test config syntax
sudo nginx -t

# Reload after config changes
sudo systemctl reload nginx

# Full restart if reload fails
sudo systemctl restart nginx

# Check logs
sudo tail -100 /var/log/nginx/error.log
```

### 4. Ingestion failures

**Symptom:** Nightly ingest not running or failing.

```bash
# Check timer
sudo systemctl list-timers --all | grep deltastack-ingest

# Manual run
cd /home/ec2-user/apps/deltastack
source venv/bin/activate
python -m deltastack --ticker AAPL --start 2025-01-01 --end 2026-01-01

# Check via API
curl -s https://api.deltastack.ai/ingest/status -H "X-API-Key: <KEY>"
```

**Polygon rate limits:**
- Free tier: 5 API calls/minute
- The retry/backoff system handles 429 responses automatically
- If persistent, increase `HTTP_BACKOFF_BASE` in `.env`

### 5. DuckDB file corruption

**Symptom:** API returns 500 errors on backtest/trade endpoints.

**Recovery:**
```bash
# Stop the API
sudo systemctl stop deltastack-api

# Check DB file
ls -la /home/ec2-user/data/deltastack/deltastack.duckdb

# Option A: Restore from backup
LATEST=$(ls -1d /home/ec2-user/data/deltastack/backups/*/ | tail -1)
cp "${LATEST}/deltastack.duckdb" /home/ec2-user/data/deltastack/deltastack.duckdb

# Option B: Delete and let app recreate tables (loses history)
rm /home/ec2-user/data/deltastack/deltastack.duckdb

# Restart
sudo systemctl start deltastack-api
```

### 6. Disk space issues

```bash
# Check disk usage
df -h
du -sh /home/ec2-user/data/deltastack/*

# Check via API
curl -s https://api.deltastack.ai/stats/storage -H "X-API-Key: <KEY>"

# Prune old backups manually
find /home/ec2-user/data/deltastack/backups -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \;
```

### 7. Trading endpoint returns 503

This is **expected** when `TRADING_ENABLED=false` (the default).

To enable paper trading:
```bash
# Edit .env
nano /home/ec2-user/apps/deltastack/.env
# Set: TRADING_ENABLED=true

# Restart
sudo systemctl restart deltastack-api
```

To disable again:
```bash
# Set: TRADING_ENABLED=false
sudo systemctl restart deltastack-api
```

---

## Backup & Recovery

### Automated backups
- **Schedule:** Daily at 02:00 UTC via `deltastack-backup.timer`
- **Location:** `/home/ec2-user/data/deltastack/backups/YYYY-MM-DD_HHMMSS/`
- **Retention:** 7 days (auto-pruned)
- **Contents:** DuckDB file, metadata JSONs, Parquet bar files

### Install backup timer
```bash
sudo cp /home/ec2-user/apps/deltastack/systemd/deltastack-backup.service /etc/systemd/system/
sudo cp /home/ec2-user/apps/deltastack/systemd/deltastack-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now deltastack-backup.timer
```

### Manual backup
```bash
bash /home/ec2-user/apps/deltastack/scripts/backup.sh
```

---

## Log Management

### journalctl
```bash
# View API logs
sudo journalctl -u deltastack-api -f            # follow live
sudo journalctl -u deltastack-api --since today  # today's logs
sudo journalctl -u deltastack-api --since "1 hour ago"

# Vacuum old logs (keep 500MB)
sudo journalctl --vacuum-size=500M
```

### Nginx logs
```bash
# Access logs
sudo tail -f /var/log/nginx/access.log

# Error logs
sudo tail -f /var/log/nginx/error.log

# Rotate manually
sudo logrotate -f /etc/logrotate.d/nginx
```

---

## Performance Monitoring

### API metrics
```bash
curl -s https://api.deltastack.ai/metrics/basic
```

### Storage stats
```bash
curl -s https://api.deltastack.ai/stats/storage -H "X-API-Key: <KEY>"
```

### System resources
```bash
# CPU/memory
top -bn1 | head -20

# Disk I/O
iostat -x 1 3

# Memory detail
free -h
```

---

## Security Checklist

- [ ] `.env` file has `chmod 600`
- [ ] `DELTASTACK_API_KEY` is set to a strong random value
- [ ] `TRADING_ENABLED=false` unless actively paper trading
- [ ] Port 8000 is NOT open in security group
- [ ] Only ports 22, 80, 443 are open
- [ ] SSH key access only (no password auth)
- [ ] certbot auto-renewal verified (`--dry-run`)
