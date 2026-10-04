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
