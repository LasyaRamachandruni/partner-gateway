"""Service level objectives and multi-window burn-rate alerting.

SLIs (measured from the gateway's own metrics):
  availability   share of partner requests that don't fail because of us (5xx), excluding 4xx
  latency        share of read requests answered within 300 ms
  command        share of accepted commands that reach a final state within 60 s

Burn rate = observed error rate / error budget. A burn rate of 1 spends exactly
the 30-day budget in 30 days. Alerts follow the multi-window, multi-burn-rate
pattern from the Google SRE workbook. Each alert needs a long window, so it's
significant, and a short window, so it resets quickly once fixed:

  page    burn > 14.4 over 1h and 5m    (2% of the monthly budget in an hour)
  page    burn > 6    over 6h and 30m   (5% in six hours)
  ticket  burn > 1    over 3d and 6h    (on track to miss the objective)

The same thresholds are in deploy/prometheus/slo-rules.yml. This module is the
reference implementation, and the tests pin it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SLO:
    name: str
    objective: float  # e.g. 0.999
    description: str

    @property
    def error_budget(self) -> float:
        return 1 - self.objective


SLOS = {
    "availability": SLO("availability", 0.999, "partner requests not failed by the gateway (non-5xx)"),
    "latency": SLO("latency", 0.99, "vehicle state reads answered within 300 ms"),
    "command": SLO("command", 0.99, "accepted commands reaching a final state within 60 s"),
}


@dataclass(frozen=True)
class AlertRule:
    severity: str
    burn: float
    long_window: str
    short_window: str


RULES = [
    AlertRule("page", 14.4, "1h", "5m"),
    AlertRule("page", 6.0, "6h", "30m"),
    AlertRule("ticket", 1.0, "3d", "6h"),
]


def burn_rate(bad: float, total: float, slo: SLO) -> float:
    if total <= 0:
        return 0.0
    return (bad / total) / slo.error_budget


def evaluate(slo: SLO, windows: dict[str, tuple[float, float]]) -> list[AlertRule]:
    """`windows` maps a window name ('1h', '5m', ...) to (bad, total). Returns the rules that fire."""
    firing = []
    for rule in RULES:
        if rule.long_window not in windows or rule.short_window not in windows:
            continue
        if (burn_rate(*windows[rule.long_window], slo) > rule.burn
                and burn_rate(*windows[rule.short_window], slo) > rule.burn):
            firing.append(rule)
    return firing


def budget_remaining(bad: float, total: float, slo: SLO) -> float:
    """Share of the period's error budget still unspent (negative when overspent)."""
    if total <= 0:
        return 1.0
    return 1 - (bad / total) / slo.error_budget


# -- Prometheus rules ------------------------------------------------------------------------
# deploy/prometheus/slo-rules.yml is generated from the definitions above (`python -m pgw.obs.slo`),
# and a test checks the committed file matches, so the alerts can't drift from the SLOs.

API_ROUTES = '/v1/.*|/oauth/token'
WINDOWS = ("5m", "30m", "1h", "6h", "3d")


def _sli_expr(name: str, w: str) -> str:
    if name == "availability":
        sel = f'route=~"{API_ROUTES}"'
        return (f'sum(rate(pgw_http_requests_total{{{sel},status=~"5.."}}[{w}])) / '
                f'sum(rate(pgw_http_requests_total{{{sel}}}[{w}]))')
    if name == "latency":
        sel = 'route="/v1/vehicles/{vin}",method="GET"'
        return (f'1 - sum(rate(pgw_http_request_duration_seconds_bucket{{{sel},le="0.3"}}[{w}])) / '
                f'sum(rate(pgw_http_request_duration_seconds_count{{{sel}}}[{w}]))')
    return ('1 - sum(rate(pgw_command_completion_seconds_bucket{le="60"}[' + w + '])) / '
            'sum(rate(pgw_command_completion_seconds_count[' + w + ']))')


def prometheus_rules() -> dict:
    recording, alerts = [], []
    for name, slo in SLOS.items():
        for w in WINDOWS:
            recording.append({"record": f"pgw:slo_{name}_error_ratio:rate{w}", "expr": _sli_expr(name, w)})
        for rule in RULES:
            threshold = round(rule.burn * slo.error_budget, 6)
            alerts.append({
                "alert": f"PartnerGateway{name.title()}BudgetBurn{rule.long_window}",
                "expr": (f"pgw:slo_{name}_error_ratio:rate{rule.long_window} > {threshold} and "
                         f"pgw:slo_{name}_error_ratio:rate{rule.short_window} > {threshold}"),
                "labels": {"severity": rule.severity, "slo": name},
                "annotations": {
                    "summary": f"{slo.description}: burning error budget at over {rule.burn}x "
                               f"({rule.long_window} and {rule.short_window} windows)",
                    "runbook": f"docs/RUNBOOK.md#{name}"},
            })
    alerts += [
        {"alert": "PartnerGatewayCircuitOpen", "expr": 'max(pgw_circuit_state{name="vehicle-service"}) == 2',
         "for": "2m", "labels": {"severity": "ticket", "slo": "availability"},
         "annotations": {"summary": "circuit to the vehicle service has been open for 2 minutes",
                         "runbook": "docs/RUNBOOK.md#circuit-open"}},
        {"alert": "PartnerGatewayWebhookDeadLetters",
         "expr": 'increase(pgw_webhook_deliveries_total{outcome="dead_lettered"}[15m]) > 0',
         "labels": {"severity": "ticket"},
         "annotations": {"summary": "webhook deliveries are being dead-lettered",
                         "runbook": "docs/RUNBOOK.md#webhook-dead-letters"}},
    ]
    return {"groups": [{"name": "partner-gateway-slo-recording", "rules": recording},
                       {"name": "partner-gateway-slo-alerts", "rules": alerts}]}


def render_rules() -> str:
    import yaml

    header = "# Generated by `python -m pgw.obs.slo` from pgw/obs/slo.py. Do not edit by hand.\n"
    return header + yaml.safe_dump(prometheus_rules(), sort_keys=False, width=200)


if __name__ == "__main__":
    from pathlib import Path

    Path("deploy/prometheus/slo-rules.yml").write_text(render_rules())
    print("wrote deploy/prometheus/slo-rules.yml")
