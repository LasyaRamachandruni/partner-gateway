#!/usr/bin/env bash
# Run the load test against a local multi-replica stack started as plain processes:
# one vehicle service and GATEWAYS gateway replicas sharing one Redis and one Postgres.
# (docker compose runs the same topology in containers; see docker-compose.yml.)
#
#   PGW_DATABASE_URL=postgresql+asyncpg://postgres@127.0.0.1/pgw PGW_REDIS_URL=redis://127.0.0.1:6379/0 \
#   RATE=300 DURATION=60s GATEWAYS=2 loadtest/run-local.sh
#
# RESET_DB=1 empties the gateway's tables first (only use it on a scratch database).
#
# Needs: pip install -e ".[dev]", k6 on PATH, Redis and Postgres reachable at the URLs above.
set -euo pipefail

: "${PGW_DATABASE_URL:?set PGW_DATABASE_URL}"
: "${PGW_REDIS_URL:?set PGW_REDIS_URL}"
RATE=${RATE:-300}
DURATION=${DURATION:-60s}
GATEWAYS=${GATEWAYS:-2}
OUT=${OUT:-loadtest/results/local-$(date -u +%Y%m%dT%H%M%SZ)}
WORK=$(mktemp -d)
mkdir -p "$OUT"
export PGW_BUS=redis PGW_LOG_LEVEL=WARNING PGW_CERTS_DIR="$WORK/certs" PGW_SIGNING_KEYS_FILE="$WORK/keyset.json"

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

pgw certs --out "$WORK/certs" >/dev/null
pgw keys init "$WORK/keyset.json" >/dev/null
pgw db upgrade
if [ "${RESET_DB:-0}" = 1 ]; then  # start from empty tables (destroys data in that database)
  psql "$(echo "$PGW_DATABASE_URL" | sed 's/+asyncpg//')" -qc "TRUNCATE partners, commands, webhook_deliveries"
fi
redis-cli -u "$PGW_REDIS_URL" --scan --pattern 'pgw:*' | xargs -r redis-cli -u "$PGW_REDIS_URL" del >/dev/null

PGW_TIME_SCALE=0.05 pgw vehicle-service --port 50051 --vehicles 50 >"$OUT/vehicle-service.log" 2>&1 &
pids+=($!)
urls=()
for i in $(seq 0 $((GATEWAYS - 1))); do
  port=$((8080 + i))
  PGW_VEHICLE_SERVICE=127.0.0.1:50051 pgw gateway --port "$port" >"$OUT/gateway-$i.log" 2>&1 &
  pids+=($!)
  urls+=("http://127.0.0.1:$port")
done
for u in "${urls[@]}"; do
  for _ in $(seq 1 100); do curl -sf "$u/readyz" >/dev/null && break; sleep 0.2; done
done

creds=$(pgw partners create --name "Load test" --demo-vins 50 --rate $((RATE * 4)) --burst $((RATE * 8)))
client_id=$(echo "$creds" | python -c 'import json,sys; print(json.load(sys.stdin)["client_id"])')
client_secret=$(echo "$creds" | python -c 'import json,sys; print(json.load(sys.stdin)["client_secret"])')

{
  echo "date_utc: $(date -u +%FT%TZ)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs) x $(nproc) vCPU"
  echo "memory_gb: $(awk '/MemTotal/ {printf "%.1f", $2/1048576}' /proc/meminfo)"
  echo "effective_parallel_cpus: $(python - <<'PY'
# nproc can overstate what a container or VM may use: measure how much work N busy processes get done
import multiprocessing as mp, os, time
def spin(_):
    end, n = time.perf_counter() + 1.0, 0
    while time.perf_counter() < end:
        n += 1
    return n
with mp.Pool(1) as p:
    one = p.map(spin, [0])[0]
k = os.cpu_count() or 1
with mp.Pool(k) as p:
    many = sum(p.map(spin, [0] * k))
print(round(many / one, 1))
PY
)"
  echo "python: $(python -V 2>&1)"
  echo "k6: $(k6 version | head -1)"
  echo "topology: $GATEWAYS gateway replicas (1 uvicorn worker each) + 1 vehicle service, Redis, Postgres, all on this host"
  echo "command: RATE=$RATE DURATION=$DURATION GATEWAYS=$GATEWAYS loadtest/run-local.sh"
} >"$OUT/environment.txt"

set +e
k6 run --quiet --no-usage-report -e BASE_URLS="$(IFS=,; echo "${urls[*]}")" -e CLIENT_ID="$client_id" -e CLIENT_SECRET="$client_secret" \
  -e RATE="$RATE" -e DURATION="$DURATION" -e SUMMARY_PATH="$OUT/k6-summary.json" loadtest/gateway.js | tee "$OUT/k6.txt"
status=${PIPESTATUS[0]}
set -e
for u in "${urls[@]}"; do curl -s "$u/metrics" | grep -E '^pgw_(http_requests_total|rate_limited_total|commands_total|events_consumed_total)' ; done >"$OUT/gateway-metrics.txt"
echo "results in $OUT (k6 exit $status: 0 = all thresholds passed)"
exit "$status"
