#!/bin/bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PORT="${PORT:-8082}"

if ! "$PYTHON_BIN" - <<'PY'
import sys

raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)
PY
then
  echo "Frontend requires Python 3.11. Activate frontend_env or set PYTHON_BIN to a Python 3.11 interpreter." >&2
  exit 1
fi

cd "$REPO_ROOT"
"$PYTHON_BIN" -m chainlit run src/frontend/chainlit_app.py -w --port "$PORT"
