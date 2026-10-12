# Runbook

For whoever is on call. Each alert links here. Start with the dashboard queries;
every section ends with how to confirm it's fixed.

Useful queries:

```promql
sum by (route, status) (rate(pgw_http_requests_total[5m]))                      # traffic and errors by route
sum by (method, outcome) (rate(pgw_upstream_calls_total[5m]))                   # vehicle service results by gRPC code
max(pgw_circuit_state{name="vehicle-service"})                                  # 0 closed, 1 half open, 2 open
histogram_quantile(0.95, sum by (le) (rate(pgw_http_request_duration_seconds_bucket{route="/v1/vehicles/{vin}"}[5m])))
sum by (outcome) (rate(pgw_webhook_deliveries_total[15m]))
sum by (topic, outcome) (rate(pgw_events_consumed_total[5m]))
```

## availability

`PartnerGatewayAvailabilityBudgetBurn*`: partners are getting 5xx.

1. **Look at the status mix.** `503 upstream_unavailable` means the vehicle service is
   the cause (go to [circuit-open](#circuit-open)). Plain `500`s mean a gateway bug:
   check the logs for the `request_id` in the error body.
2. **Check a deploy.** Did one just ship? Roll it back first, then investigate.
3. **Check one partner.** If a single partner is affected, a misbehaving client usually
   shows up as 429s, which don't burn the budget. Persistent 5xx for one partner
   point at its data, for example a VIN grant for a vehicle the fleet no longer has.

**Fixed when** the 5m error ratio is back under 0.1% and the page resolves on its own.

## latency

`PartnerGatewayLatencyBudgetBurn*`: vehicle reads are slower than 300 ms.

1. **Compare gateway latency with `pgw_upstream_duration_seconds`.** If the upstream
   is slow too, the vehicle service is the cause.
2. **Check retries.** A rise in `pgw_upstream_retries_total` means slowness from retries:
   the service is failing transiently, and each request pays for several attempts.
3. **Check the gateway itself.** If only the gateway is slow, look at CPU and event-loop
   lag on the replicas, and scale out.

## command

`PartnerGatewayCommandBudgetBurn*`: commands aren't finishing within 60 s.

1. **Check the consumer.** `pgw_events_consumed_total` should track
   `pgw_commands_total`. If events aren't being consumed, check the subscription
   backlog in Pub/Sub, and `/readyz` (`ready: false` means the consumer isn't running).
2. **Check expiries.** A rise in `pgw_commands_total{status="expired"}` with healthy
   consumption means cars are offline. That's expected in small numbers and worth
   checking if fleet-wide (a cellular outage).
3. **Check dead letters.** Messages in `vehicle-command-results-<env>-dead-letter`
   mean the consumer is rejecting them. Inspect one with the `-dead-letter-inspect` subscription.

## circuit-open

The gateway stopped calling the vehicle service because most recent calls failed.
Partners get fast `503`s with `Retry-After`, which is the intended behavior.

1. **Check the vehicle service**, not the gateway. Look at its health, recent deploys,
   and the `outcome` label on `pgw_upstream_calls_total`:
   - `UNAVAILABLE`: the service is down or overloaded.
   - `DEADLINE_EXCEEDED`: it's hanging.
2. **Check mTLS.** If calls fail with `UNAVAILABLE` immediately after a certificate
   rotation, the gateway's client certificate may have expired, or the server is
   missing the CA. Run `openssl x509 -enddate -noout` on both certificates.
3. **Let it recover.** The breaker sends probes every few seconds and closes by
   itself once they succeed. Don't restart the gateway to "reset" it, because that
   sends full traffic at a service that is still recovering.

## webhook-dead-letters

A partner's endpoint is rejecting or failing our deliveries.

1. `GET /v1/webhooks/deliveries?status=dead` (as the partner) shows the `last_error`:
   - `HTTP 4xx`: the partner rejected the event, often after rotating their secret
     without allowing for both.
   - `HTTP 5xx` or a timeout: their endpoint is down.
2. Contact the partner with the event ids and the error.
3. Once they confirm it's fixed, replay with `POST /v1/webhooks/deliveries/{id}/replay`.
   Partners deduplicate on `Pgw-Event-Id`, so replaying is safe.

## Procedures

### Rotate token signing keys

The key set lives in Secret Manager (`pgw-token-signing-key-<env>`); every replica re-reads it
every minute.

1. `make keys-stage PROJECT=...` adds the next key. It is published in the JWKS but not used.
2. Wait at least 6 minutes: 1 minute for replicas to reload, 5 for the JWKS cache.
3. `make keys-activate PROJECT=...` signs with the new key. The old key keeps verifying until
   its tokens expire (15 minutes + 1 minute of leeway).
4. After 16 minutes, `make keys-prune PROJECT=...` removes the old key.

No partner action is needed; existing tokens keep working. Locally the same steps are
`pgw keys stage|activate|prune keyset.json`.

### Rotate a partner's webhook secret

1. `PartnerRegistry.rotate_webhook_secret()` adds the new secret. Deliveries now carry
   two `v1=` signatures.
2. Give the partner the new secret. Their verifier accepts either secret.
3. When they confirm, `retire_webhook_secret()` drops the old one.

### Onboard a partner

1. `pgw partners create --name ... --vin ... --webhook-url ...` (in GCP: the `pgw-admin-<env>`
   job with `--args=partners,create,...`) prints the client secret **once**. Send it through a
   secure channel; only its hash is stored.
2. Grant only the scopes they need. Read-only integrations don't get `vehicles:command`.
3. Point them at the sandbox (`pgw sandbox`) and the SDK.

### Redis (Memorystore) is down

Every authenticated request checks a rate-limit bucket in Redis, so the API answers 500
and `/readyz` reports `"stores": "unreachable"`. Token issuance also stops (it is rate
limited per client id). Commands already accepted keep completing: results, command state
and webhooks are in Postgres.

1. Check the instance: `gcloud redis instances describe pgw-<env> --region <region>`.
2. A Basic-tier instance restarts empty. Nothing else is needed: buckets refill and
   idempotency keys start over.

### Postgres (Cloud SQL) is down

Token issuance, command creation and command status fail; reads of vehicle state keep
working for about 5 seconds per partner (the partner cache), then fail too. Results that
arrive meanwhile stay in the Pub/Sub subscription and are applied when the database is back.

1. `gcloud sql instances describe pgw-<env>`; check the connection count against the
   tier's limit (each gateway instance opens up to `PGW_DB_POOL_SIZE` x 2).
2. Webhook deliveries resume on their own: the outbox is in the database.
