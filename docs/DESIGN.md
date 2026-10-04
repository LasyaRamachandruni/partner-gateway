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

## What a production version would add

- **Shared state:** rate limits, idempotency keys and command state in a shared
  store (Redis or a database), so the gateway can run several replicas.
- **Partners and grants in a database,** with an admin API and an audit log.
- **Key storage:** signing keys and mTLS certificates from Secret Manager and a managed
  CA, with automatic rotation. The Terraform declares the secrets.
- **A compute target** (GKE, or Cloud Run where mTLS terminates at a mesh). The
  Terraform covers messaging, identity and secrets, not compute.
- **Per-partner quotas on commands** and an approval step for sensitive commands
  like unlocking.
