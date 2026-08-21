#!/bin/bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Start FastAPI backend
echo "Starting Natural Language to GraphQL Query Conversion System..."
echo "Access the system at: http://127.0.0.1:8000"
echo "Test results will be saved in the chat_history folder"
echo "========================================"

if ! "$PYTHON_BIN" - <<'PY'
import sys

raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)
PY
then
  echo "Backend requires Python 3.11. Activate backend_env or set PYTHON_BIN to a Python 3.11 interpreter." >&2
  exit 1
fi

cd "$SCRIPT_DIR"
"$PYTHON_BIN" -m uvicorn app:app --reload
