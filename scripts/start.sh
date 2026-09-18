#!/usr/bin/env bash
# Start the dashboard with auto-reload.
#
#   ./scripts/start.sh              # 127.0.0.1:8000
#   PORT=9000 ./scripts/start.sh    # custom port
#   HOST=0.0.0.0 ./scripts/start.sh # expose on the network
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"

if [[ -d ".venv" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

exec python3 -m uvicorn app.main:app --reload --host "$HOST" --port "$PORT"
