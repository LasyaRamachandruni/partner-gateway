# Service level objectives

Defined in `pgw/obs/slo.py`. The Prometheus rules in `deploy/prometheus/slo-rules.yml`
are generated from it (`python -m pgw.obs.slo`). Window: 30 days.

| SLO | SLI | Objective | Error budget (30 days) |
|---|---|---|---|
| **Availability** | Share of partner API requests (`/v1/*`, `/oauth/token`) not answered with 5xx. 4xx are the partner's errors, not ours. | 99.9% | 0.1%, about 43 minutes of full outage |
| **Latency** | Share of `GET /v1/vehicles/{vin}` answered within 300 ms | 99% | 1% |
| **Command completion** | Share of accepted commands that reach a final state within 60 s | 99% | 1% |

Command completion measures the whole pipeline: the vehicle service, the car,
Pub/Sub and the gateway's consumer. A broken consumer shows up here even when
every API request succeeds.

## Alerting: multi-window burn rates

Burn rate = observed error ratio / error budget. Burn rate 1 spends the budget
exactly over 30 days.

| Severity | Burn rate | Long window | Short window | Meaning |
|---|---:|---|---|---|
| page | 14.4 | 1h | 5m | 2% of the monthly budget gone in an hour |
| page | 6 | 6h | 30m | 5% gone in six hours |
| ticket | 1 | 3d | 6h | On course to miss the objective |

Both windows must exceed the threshold. The long window keeps a blip from paging
anyone. The short window makes the alert stop soon after the problem is fixed,
without waiting an hour for the long window to cool down.

Two supporting alerts don't depend on the SLOs:
- `PartnerGatewayCircuitOpen`: the circuit to the vehicle service has been open for 2 minutes.
- `PartnerGatewayWebhookDeadLetters`: partners are missing events.

Each alert links to its section in the [runbook](RUNBOOK.md).
