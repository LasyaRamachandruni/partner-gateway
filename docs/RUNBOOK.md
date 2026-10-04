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

1. `KeyRing.stage()` publishes the next key in the JWKS.
2. Wait at least the JWKS cache time (5 minutes) so verifiers have the key.
3. `KeyRing.rotate()` starts signing with the new key. The old key keeps verifying until
   its tokens expire (15 minutes + 1 minute of leeway), then it's pruned.

No partner action is needed; existing tokens keep working.

### Rotate a partner's webhook secret

1. `PartnerRegistry.rotate_webhook_secret()` adds the new secret. Deliveries now carry
   two `v1=` signatures.
2. Give the partner the new secret. Their verifier accepts either secret.
3. When they confirm, `retire_webhook_secret()` drops the old one.

### Onboard a partner

1. `PartnerRegistry.register(name, scopes, vins, webhook_url=...)` returns the client
   secret **once**. Send it through a secure channel; only its hash is stored.
2. Grant only the scopes they need. Read-only integrations don't get `vehicles:command`.
3. Point them at the sandbox (`pgw sandbox`) and the SDK.
