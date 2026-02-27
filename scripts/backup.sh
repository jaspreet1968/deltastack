#!/usr/bin/env bash
# DeltaStack daily backup script.
# Copies DuckDB + metadata JSON + latest parquet to a dated folder.
# Usage: bash scripts/backup.sh
# Designed to be called by systemd timer deltastack-backup.timer

set -euo pipefail

DATA_DIR="${DATA_DIR:-/home/ec2-user/data/deltastack}"
BACKUP_ROOT="${BACKUP_DIR:-${DATA_DIR}/backups}"
DATE=$(date -u +%Y-%m-%d_%H%M%S)
DEST="${BACKUP_ROOT}/${DATE}"

echo "[$(date -u)] Starting DeltaStack backup -> ${DEST}"

mkdir -p "${DEST}"

# 1. DuckDB file
DB_FILE="${DATA_DIR}/deltastack.duckdb"
if [ -f "${DB_FILE}" ]; then
    cp "${DB_FILE}" "${DEST}/deltastack.duckdb"
    echo "  Backed up DuckDB ($(du -h "${DB_FILE}" | cut -f1))"
else
    echo "  WARN: DuckDB file not found at ${DB_FILE}"
fi

# 2. Metadata JSON files
META_DIR="${DATA_DIR}/metadata"
if [ -d "${META_DIR}" ]; then
    cp -r "${META_DIR}" "${DEST}/metadata"
    echo "  Backed up metadata ($(ls "${META_DIR}" | wc -l) files)"
fi

# 3. Latest parquet for each ticker (just the data.parquet files)
BARS_DIR="${DATA_DIR}/bars/day"
if [ -d "${BARS_DIR}" ]; then
    mkdir -p "${DEST}/bars"
    find "${BARS_DIR}" -name "data.parquet" -exec cp --parents {} "${DEST}/bars/" \; 2>/dev/null || true
    echo "  Backed up bar data"
fi

# 4. Prune old backups (keep last 7 days)
if [ -d "${BACKUP_ROOT}" ]; then
    find "${BACKUP_ROOT}" -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \; 2>/dev/null || true
fi

echo "[$(date -u)] Backup complete: ${DEST}"
echo "  Total size: $(du -sh "${DEST}" | cut -f1)"
