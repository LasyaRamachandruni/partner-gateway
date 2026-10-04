# partner-gateway

**An API gateway that lets outside partners (insurers, charging networks, fleet
managers) read data from connected vehicles and send them commands, without
having direct access to the vehicles.**

Partners call a REST API with OAuth 2.0 tokens. The gateway talks to the vehicle
service over **gRPC with mutual TLS**. Commands complete asynchronously, their
results flow back as **Pub/Sub events**, and partners are notified through
**signed webhooks**. Every external call is wrapped in rate limits, deadlines,
retries, a circuit breaker and idempotency keys. Every request is traced and
measured against SLOs.

```
                     OAuth 2.0 (client credentials, ES256 JWT, rotating keys)
 partner app ──REST──►┌──────────────────────── partner gateway ─────────────────────────┐
  (partner_sdk)       │ auth ─► rate limit ─► scope + vehicle grant ─► idempotency key     │
                      │                                    │                              │
                      │          deadline ─► retry (jittered) ─► circuit breaker           │
                      └────────────────────────────────────┼──────────────────────────────┘
        ▲                                     gRPC + mTLS   │  (client cert, CN allow-list)
        │                                                   ▼
        │ signed webhook                        ┌──── vehicle service ────┐      cars (simulated):
        │ (HMAC, retries,                       │ dedupe command ids      │◄───► online / offline,
        │  dead letters, replay)                │ queue for offline cars  │      latency, failures
        │                                       └────────────┬────────────┘
        │                                                    │ publish result
        └──── gateway consumer (idempotent) ◄── Pub/Sub ─────┘ vehicle-command-results (+ dead-letter topic)

 one trace across all hops (W3C traceparent in headers, gRPC metadata, message attributes)
 Prometheus metrics → SLOs → multi-window burn-rate alerts
```

> The vehicles are simulated (`pgw/vehicles/fleet.py`): they go online and offline,
> respond with variable latency, and fail commands the way cars do (charging an
> unplugged car). Everything between the partner and the simulated car is the real
> implementation: tokens, gRPC, mTLS, Pub/Sub, webhooks.

## What's in it

| Area | What's implemented |
|---|---|
| **Partner auth** | OAuth 2.0 client credentials. Secrets are stored only as scrypt hashes. Tokens are ES256 JWTs with `kid`. The algorithm is pinned (no `alg=none` or HS256 confusion). Scopes are `vehicles:read` and `vehicles:command`. |
| **Key rotation** | Signing keys are staged in the JWKS before use. Old keys verify until their tokens expire, then are pruned, so a rotation never rejects a valid token. |
| **Authorization** | Each partner sees only the VINs that owners granted it. Other VINs return 404, not 403, so a partner can't tell which vehicles exist. |
| **Service-to-service** | gRPC over mTLS with an internal CA. The vehicle service requires a client certificate and checks the caller's identity against an allow-list. |
| **Commands** | Accepted with `202`, run when the car is online (queued if it's offline), and expire if the car never reconnects. The vehicle service deduplicates command ids, so retries are safe. |
| **Events** | Pub/Sub topics with at-least-once delivery. Consumers are idempotent, and messages that keep failing go to a dead-letter topic. The bus is in-memory for tests and Google Cloud Pub/Sub (or its emulator) for deployment. |
| **Webhooks** | HMAC-SHA256 signatures with timestamps, which block replays. Multiple signatures during secret rotation. Retries with backoff, dead letters, and a replay API. |
| **Resilience** | Per-partner token buckets with `Retry-After`, per-call deadlines, retries with jittered backoff inside an overall deadline, a circuit breaker with single-shot probes, and `Idempotency-Key` for commands. |
| **Observability** | OpenTelemetry traces from the partner's request to the webhook, Prometheus metrics labelled by route template, and SLOs with burn-rate alert rules generated from code. |
| **Partner SDK** | Caches and refreshes tokens, honors `Retry-After`, adds idempotency keys automatically, and verifies webhook signatures. |
| **Infrastructure** | Docker image, and docker-compose with the Pub/Sub emulator, Prometheus and Jaeger. Terraform for Pub/Sub with dead letters, least-privilege service accounts, Secret Manager and Artifact Registry. |
| **Operations** | [Runbook](docs/RUNBOOK.md), [SLOs](docs/SLO.md), [design notes](docs/DESIGN.md). |

## Results: what the resilience patterns are worth

`pgw chaos` runs the whole system in one process (real gRPC over mTLS) with 20
concurrent partner clients. It breaks the vehicle service on purpose and compares
three gateway configurations. Each number is the median of 3 runs.
[Full report](docs/results/CHAOS.md).

**30% of vehicle-service calls fail:**

| Gateway | Success | p95 latency |
|---|---:|---:|
| No retries, no breaker | 71.1% | 34 ms |
| Retries + circuit breaker | **97.6%** | 78 ms |

Retries recover almost all failed calls, which matches the theory: three attempts at a
30% failure rate leave 2.7% of requests failing. They cost about 40 ms at p95 and
about 1.4 calls to the vehicle service per request.

**The vehicle service hangs for 2 seconds:**

| Gateway | Median response while down | Calls sent to the hung service |
|---|---:|---:|
| No retries, no breaker | 255 ms (waits for the deadline) | 160 |
| Retries only | 674 ms (waits for several deadlines) | 160 |
| Retries + circuit breaker | **12 ms** (fails fast with `Retry-After`) | **75** |

Retries alone make an outage *worse* for partners: they wait through several
timeouts. The breaker answers in milliseconds and halves the load on the failing
service. The cost is real and measured: right after the service recovers, the
breaker keeps failing some requests until its probes succeed. Success in the 2 s
after recovery was 94–96% with the breaker and 100% without it.

![Outage timeline](docs/results/outage_timeline.png)

## Run it

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                         # 49 tests (+2 that need the Pub/Sub emulator)

pgw sandbox                    # everything in one process; prints a demo partner's credentials
pgw chaos --out results/       # the experiment above (about 90 s)
```

With the sandbox running:

```bash
TOKEN=$(curl -s -u $CLIENT_ID:$CLIENT_SECRET -d grant_type=client_credentials localhost:8080/oauth/token | jq -r .access_token)
curl -H "Authorization: Bearer $TOKEN" localhost:8080/v1/vehicles
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $(uuidgen)" \
     -H "Content-Type: application/json" -d '{"type":"UNLOCK"}' localhost:8080/v1/vehicles/$VIN/commands
# the sandbox prints the signed webhook when the car finishes
```

As separate services, with the Pub/Sub emulator, Prometheus (SLO rules loaded) and Jaeger:

```bash
docker compose up --build
```

From Python:

```python
from partner_sdk import PartnerClient

async with PartnerClient("http://localhost:8080", client_id, client_secret) as pc:
    cmd = await pc.send_command(vin, "START_CHARGING")      # retried safely: same idempotency key
    result = await pc.wait_for_command(cmd["command_id"])
```

## API

| Endpoint | Purpose |
|---|---|
| `POST /oauth/token` | Client credentials → access token (Basic auth or form fields) |
| `GET /.well-known/jwks.json` | Public signing keys |
| `GET /v1/vehicles` | VINs this partner may access |
| `GET /v1/vehicles/{vin}` | Current state (`vehicles:read`) |
| `POST /v1/vehicles/{vin}/commands` | `LOCK`, `UNLOCK`, `START_CHARGING`, `STOP_CHARGING`, `HONK_AND_FLASH` → `202` + command id (`vehicles:command`; `Idempotency-Key` supported) |
| `GET /v1/commands/{id}` | `pending`, `succeeded`, `failed` or `expired`, with the reason |
| `POST /v1/webhooks/test` | Send a test event |
| `GET /v1/webhooks/deliveries` · `POST …/{id}/replay` | Delivery log; replay dead letters |
| `GET /healthz` · `/readyz` · `/metrics` | Liveness, readiness, Prometheus |

Errors share one shape, `{"error": {"code", "message", "request_id"}}`, and every response carries `X-Request-Id`.

## Tests

49 tests. The end-to-end ones run the real gateway against the real vehicle service
over gRPC with mTLS. They cover:
- **OAuth:** token errors, a forged signature, `alg=none`, a token signed by the wrong key, and key rotation without rejecting valid tokens.
- **Authorization:** scopes, VIN grants, and VINs the partner can't see.
- **mTLS:** connections without a certificate, with a certificate from another CA, and with an identity that isn't allowed.
- **Commands:** idempotency, offline cars, failures, and lost results that expire.
- **Webhooks:** retries, dead letters, replay, signature checks and secret rotation.
- **Events:** duplicate events, including a run where half of all events are redelivered.
- **Tracing:** one trace id from the request through gRPC and Pub/Sub to the webhook.
- **Metrics:** labelled by route template.
- **Smaller pieces:** the rate limiter, retry, breaker and idempotency store on their own, plus the SDK's retry and token refresh.
- **SLO alerts:** burn-rate math, and checks that the committed Prometheus rules match the SLO definitions.

On every push, CI also:
- runs the Pub/Sub tests against the real emulator;
- runs a short chaos experiment and asserts the breaker's effect;
- runs `terraform validate` and `promtool check rules`;
- builds the Docker image.

## Layout

```
pgw/
  proto/          vehicle.proto + generated gRPC stubs
  auth/           partners (hashed secrets, grants, webhook secrets), signing keys, tokens
  resilience/     rate limiter, retry, circuit breaker, idempotency store
  bus/            Bus interface; in-memory and Google Cloud Pub/Sub implementations
  vehicles/       simulated fleet; gRPC vehicle service with mTLS and chaos controls
  gateway/        REST app, vehicle client, command store, webhook dispatcher
  obs/            metrics, tracing, SLOs (+ Prometheus rule generation)
  certs.py        internal CA for mTLS
  chaos.py        the resilience experiment
  system.py       wires everything together (tests, sandbox, chaos)
partner_sdk/      client + webhook signature verification
deploy/           terraform/ (GCP), prometheus/ (scrape config, generated SLO rules)
docs/             DESIGN.md, RUNBOOK.md, SLO.md, results/
```
