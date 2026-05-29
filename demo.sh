#!/usr/bin/env bash
# End-to-end restart demo for the append-only event store.
# Usage: ./demo.sh   (run from the event_store/ directory with the venv active)
set -euo pipefail

BASE="http://127.0.0.1:8000"
LOG="${EVENTS_LOG:-events.log}"

start_server() {
  uvicorn main:app --port 8000 >server.out 2>&1 &
  SERVER_PID=$!
  # wait for it to come up
  for _ in $(seq 1 50); do
    curl -sf "$BASE/stats" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server failed to start"; cat server.out; exit 1
}

stop_server() {
  kill "$SERVER_PID" 2>/dev/null || true
  # give it a moment to release the port; don't block on wait
  for _ in $(seq 1 30); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.1
  done
}

echo "=== fresh start ==="
rm -f "$LOG"
start_server

echo "--- writing 3 events (one with unicode) ---"
ID1=$(curl -s -X POST "$BASE/events" -H 'Content-Type: application/json' \
  -d '{"type":"signup","user":"chidi"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
ID2=$(curl -s -X POST "$BASE/events" -H 'Content-Type: application/json' \
  -d '{"type":"purchase","amount":42}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
ID3=$(curl -s -X POST "$BASE/events" -H 'Content-Type: application/json' \
  -d '{"type":"note","text":"café — naïve 日本語"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
echo "ID1=$ID1"; echo "ID2=$ID2"; echo "ID3=$ID3"

echo "--- stats before restart ---"
curl -s "$BASE/stats"; echo

stop_server
echo "=== server stopped, restarting (recovery should replay the log) ==="
start_server

echo "--- reads after restart ---"
echo "GET ID1:"; curl -s "$BASE/events/$ID1"; echo
echo "GET ID3 (unicode):"; curl -s "$BASE/events/$ID3"; echo
echo "GET missing id -> expect 404:"; curl -s -o /dev/null -w '%{http_code}\n' "$BASE/events/does-not-exist"
echo "--- stats after restart ---"
curl -s "$BASE/stats"; echo

stop_server
echo "=== done ==="
