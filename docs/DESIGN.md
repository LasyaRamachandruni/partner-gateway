# Design notes

Why the gateway is built the way it is, one decision at a time.

## REST outside, gRPC inside

Partners integrate with plain HTTPS and JSON, so any language works and an
SDK is a convenience rather than a requirement. Between services, gRPC gives a typed
contract (`vehicle.proto`), deadlines that propagate, and status codes that map
cleanly to retry decisions: `UNAVAILABLE` is worth retrying, `NOT_FOUND` is not.

## Commands are asynchronous

A car can be parked in a garage with no signal. Holding an HTTP request open until
it reconnects would tie up connections and turn the partner's timeouts into wrong
answers. So `POST /commands` returns `202` with a command id as soon as the
vehicle service has accepted the command. The result arrives later as an event,
and the partner learns about it from a webhook or by polling. Every command ends
in exactly one final state: `succeeded`, `failed` or `expired`.

## Retrying is only safe because every layer is idempotent

| Layer | What could go wrong on retry | Protection |
|---|---|---|
| Partner → gateway | The response to "unlock" is lost and the partner retries | `Idempotency-Key`: the same key and body replays the stored response |
| Gateway → vehicle service | A gRPC call times out after the service accepted it | The gateway assigns the command id. The service ignores ids it has seen. |
| Vehicle service → gateway | Pub/Sub redelivers a result | The consumer ignores results for commands that are already final |
| Gateway → partner | A webhook is retried after the partner processed it | The same `Pgw-Event-Id` on every attempt lets partners deduplicate |

## Retries, deadlines and the breaker work together

- **Every attempt has a deadline** (250 ms in the experiments). Without one, a
  hung service holds every request until the partner gives up.
- **Retries use full jitter inside an overall deadline.** Jitter spreads retries
  out, and the overall deadline caps how long a partner waits, however many attempts remain.
- **Only transient errors are retried.** A 404 from the vehicle service won't change on the next try.
- **The circuit breaker** trips when at least half of the last 20 calls failed, with 10 calls minimum.
  - While open, it answers `503` with `Retry-After` in milliseconds.
  - After `open_s`, it lets a few probes through. Each probe is a **single attempt**:
    retrying probes kept the circuit undecided for a full retry deadline and lowered
    success after recovery. The chaos experiment caught this.
  - Client errors don't count as failures.

The experiment shows why the breaker is necessary. During a hang, retries alone
made partners wait 674 ms per request instead of 255 ms. The breaker cut it to 12 ms
and halved the calls hitting the broken service.

## Security choices

- **Secrets:** client secrets are stored as salted scrypt hashes and compared in
  constant time. The hash runs even for unknown client ids, so response time doesn't reveal which ids exist.
- **Tokens:**
  - they are signed with ES256, and verification pins the algorithm, so `alg=none`
    and RS/HS confusion are rejected;
  - `iss`, `aud`, `exp` and `iat` are all required;
  - tokens are short-lived (15 minutes), so there's no revocation list to keep consistent.
- **Key rotation** stages the new key in the JWKS before signing with it. Verifiers that
  cache the JWKS for 5 minutes learn it before seeing a token signed with it.
- **mTLS** between services, with an allow-list of client identities: being inside
  the network isn't enough to command a car.
- **No information leaks from authorization:** a VIN the partner has no grant for
  returns the same 404 as one that doesn't exist, and the gateway rejects it before
  calling the vehicle service.
- **Webhook signatures** cover the timestamp, so a captured delivery can't be replayed later.
  Rotating a partner's secret signs with both secrets for a while, so partners can
  switch without missing events.
- **Token endpoint rate limit:** it's limited per client id to slow down secret guessing.

## Observability is designed for the SLOs

- **Metrics are labelled by route template** (`/v1/vehicles/{vin}`), never by raw
  path, so cardinality stays bounded.
- **The latency histogram has a 300 ms bucket**, because 300 ms is the latency
  SLO threshold. Without that bucket, the SLI could only be estimated.
- **Alert rules are generated from `pgw/obs/slo.py`,** and a test checks the committed
  YAML matches. The thresholds can't drift from the objectives.
- **One trace per command,** from the partner's request through gRPC and Pub/Sub to
  the webhook. When a partner says "my unlock never arrived," one trace id answers where it stopped.

## State lives outside the process, so replicas are interchangeable

Every piece of state a request depends on sits behind an interface in `pgw/store`, with an
in-memory implementation for tests and the sandbox and a shared one for deployment:

| State | Shared store | Why there |
|---|---|---|
| Rate-limit token buckets | Redis, one Lua script per decision | Checked on every request: needs to be fast, and a lost bucket only resets a limit |
| Idempotency keys | Redis, Lua for "claim the key or report what holds it" | Short-lived (24 h TTL), checked on every command |
| Partners | Postgres | Durable, audited records; read through a 5-second cache per replica |
| Commands | Postgres | Partners poll them; the state machine must survive restarts |
| Webhook deliveries | Postgres (an outbox) | A delivery must not be lost when a replica dies |

The guarantees that used to come from "one process" now come from single atomic operations
in the store, and `tests/test_multi_replica.py` checks them with two real replicas:

- **Rate limits:** the token bucket is one Lua script, so a partner gets its burst once,
  not once per replica. The control test shows two in-memory replicas letting 2x through.
- **Idempotency:** claiming a key is one Lua script. Of concurrent requests with the same key
  on different replicas, exactly one runs; the rest get 409 and later replay its response.
  An in-progress claim holds the key for a 30-second lease only, so a replica that dies
  mid-request doesn't block the key for a day.
- **Command results:** `UPDATE ... WHERE status = 'pending' RETURNING`. Two replicas that
  both receive a result (redelivery, or one per replica after a rebalance) can't both apply it,
  and only the one that did sends the webhook.
- **Webhooks:** replicas claim due deliveries with `FOR UPDATE SKIP LOCKED` and a lease.
  A replica that dies mid-attempt loses nothing; the lease runs out and another picks it up.

Redis holds nothing that can't be lost: if Memorystore restarts, buckets refill and
in-flight idempotency keys are forgotten (a retry after that could create a second
command; the vehicle service still deduplicates by command id).

## Signing keys come from a key set, not from the process

A replica that generated its own key would sign tokens no other replica can verify. Keys
are a key set (JSON: the active kid plus every key still allowed to verify) loaded by a
`KeyProvider`: an environment variable or file locally, Google Secret Manager in the cloud.
Every replica reloads it every minute and keeps its current keys if a reload fails.
Rotation is three edits of the key set (stage, activate, prune), each a new secret version;
see the runbook.

KMS asymmetric signing (the private key never leaves KMS) would be the next step. It costs
a KMS call per token, which is fine at token-issuance rates; Secret Manager keeps the
code simpler for now.

## Cloud Run replaces mTLS between the services with IAM

Cloud Run terminates TLS at its front end and can't forward a client certificate, so the
mTLS hop can't survive there. On Cloud Run the vehicle service listens in plaintext HTTP/2
behind the platform, accepts only callers with `roles/run.invoker` (the gateway's service
account), and the gateway attaches a Google-signed ID token to each call. mTLS stays the
default everywhere else (docker compose, VMs, GKE), and the same code supports both
(`PGW_VEHICLE_AUTH`, `PGW_GRPC_TLS`).

## Vehicles: simulated, or Smartcar

The vehicle service talks to "the cars" through a `VehicleBackend`. The simulated fleet
is the default. `PGW_VEHICLE_BACKEND=smartcar` uses the Smartcar API, which also serves
Smartcar's Vehicle Simulator, so the same gateway can drive simulated cars that a third
party hosts. Smartcar commands are synchronous HTTP calls; the backend runs them in the
background and publishes the result like the fleet does, so the gateway can't tell the
backends apart. It reports Smartcar errors as UNAVAILABLE rather than inventing values
for a car it couldn't read.

## What a production version would add

- **An admin API** for partners and grants, with an audit log (today: `pgw partners create`).
- **KMS-backed token signing** and a managed CA for the non-Cloud Run mTLS path.
- **Per-partner quotas on commands** and an approval step for sensitive commands
  like unlocking.
- **Redis with a replica** (Memorystore Standard tier) if losing idempotency keys on a
  Redis restart is not acceptable.
