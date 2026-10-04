"""Prometheus metrics for the gateway.

Each gateway instance owns its registry (no process-global state), so tests can run
several gateways side by side. The metrics feed the SLOs in docs/SLO.md and the
alert rules in deploy/prometheus/slo-rules.yml.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.3, 0.5, 1, 2.5, 5, 10)  # 0.3: the latency SLO threshold
BREAKER_STATE = {"closed": 0, "half_open": 1, "open": 2}


class Metrics:
    def __init__(self):
        r = self.registry = CollectorRegistry()
        self.requests = Counter("pgw_http_requests_total", "Partner API requests", ["route", "method", "status"], registry=r)
        self.latency = Histogram("pgw_http_request_duration_seconds", "Partner API latency", ["route", "method"],
                                 buckets=LATENCY_BUCKETS, registry=r)
        self.rate_limited = Counter("pgw_rate_limited_total", "Requests rejected by rate limits", ["partner"], registry=r)
        self.upstream = Counter("pgw_upstream_calls_total", "Calls to the vehicle service", ["method", "outcome"], registry=r)
        self.upstream_latency = Histogram("pgw_upstream_duration_seconds", "Vehicle service call latency", ["method"],
                                          buckets=LATENCY_BUCKETS, registry=r)
        self.retries = Counter("pgw_upstream_retries_total", "Retried vehicle service calls", ["method"], registry=r)
        self.breaker = Gauge("pgw_circuit_state", "0 closed, 1 half open, 2 open", ["name"], registry=r)
        self.commands = Counter("pgw_commands_total", "Commands by final status", ["type", "status"], registry=r)
        self.command_seconds = Histogram("pgw_command_completion_seconds", "Accepted to finished",
                                         buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120), registry=r)
        self.events = Counter("pgw_events_consumed_total", "Bus events handled", ["topic", "outcome"], registry=r)
        self.webhooks = Counter("pgw_webhook_deliveries_total", "Webhook deliveries", ["outcome"], registry=r)
        self.webhook_attempts = Histogram("pgw_webhook_attempts", "Attempts per delivered webhook",
                                          buckets=(1, 2, 3, 4, 5, 6, 8, 10), registry=r)

    def set_breaker(self, name: str, state) -> None:
        self.breaker.labels(name).set(BREAKER_STATE[getattr(state, "value", state)])

    def render(self) -> bytes:
        return generate_latest(self.registry)
