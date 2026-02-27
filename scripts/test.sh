#!/usr/bin/env bash
# DeltaStack test runner – runs unit + API tests via pytest.
# Usage: bash scripts/test.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== DeltaStack Test Suite ==="

# Activate venv if present
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
fi

# Run tests with verbose output
python -m pytest tests/ -v --tb=short "$@"

echo "=== All tests passed ==="
