# Local load test, 2026-10-12

Two gateway replicas (one uvicorn worker each), one vehicle service (simulated fleet,
50 vehicles), Redis and Postgres, and k6, all as processes on one small cloud VM.
Traffic mix and thresholds are in `loadtest/gateway.js`; each run is 15 s of warm-up
at a tenth of the rate, then 60 s at the target rate, alternating between the replicas.

Hardware (from each run's `environment.txt`): Intel Xeon @ 2.10 GHz, 2 vCPU, 7.8 GB RAM.
A busy-loop test in the same script measured 1.7 to 2.2 CPUs of parallel capacity, and
k6 shares those CPUs with everything it is testing.

| Target rps | Achieved rps | Requests | Dropped | 5xx + transport errors | `GET /v1/vehicles/{vin}` p50 / p95 / p99 (ms) | SLO thresholds |
|---:|---:|---:|---:|---:|---|---|
| 300 | 300 | 18,001 | 0 | 0.000% | 3.3 / 8.6 / 21.9 | pass |
| 450 | 450 | 27,000 | 0 | 0.000% | 6.8 / 33.6 / 91.8 | pass |
| 600 | 489 | 29,342 | 6,659 | 0.000% | 1040.5 / 5658.1 / 6829.4 | fail (latency, dropped iterations) |

Reproduce one row (Redis and Postgres running locally, `pip install -e ".[dev]"`, k6 installed):

    PGW_DATABASE_URL=postgresql+asyncpg://postgres@127.0.0.1/pgw PGW_REDIS_URL=redis://127.0.0.1:6379/0 \
    RATE=450 DURATION=60s GATEWAYS=2 loadtest/run-local.sh

Reading the numbers:

- This machine saturates a little under 500 requests/s for the whole stack. Past that,
  requests queue: no errors, but latency climbs and k6 drops iterations it can't start.
  It is a measure of this 2-CPU box, not of the design: the replicas share no state
  in-process, so the cloud test adds instances instead of CPU per instance.
- No run produced a 5xx, and the gateway logs (level WARNING) were empty.
- `gateway-metrics.txt` is each replica's Prometheus counters after the run. Both replicas
  served traffic and both consumed command results from the shared Redis stream. The
  2 `expired` commands in the 600 rps run were pending rows left in the database by an
  earlier, interrupted run; the expiry sweep closed them (the script now has `RESET_DB=1`
  for a clean database).
- The 1,000 requests/s target is for the Cloud Run deployment; that run is pending
  (`make loadtest-cloud` after `make deploy`).
