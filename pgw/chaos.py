"""Chaos experiment: how does the gateway behave when the vehicle service doesn't?

    pgw chaos --out results/

The whole system runs in process (real gRPC over mTLS, real gateway). Concurrent
partner clients read vehicle state while the vehicle service is made to misbehave
in two ways:

- **flaky**: 30% of calls fail at once with UNAVAILABLE.
- **outage**: healthy for 2 s, then *hangs* for 2 s (every call times out), then healthy again.

Each scenario runs against three gateway configurations:

- **none**: one attempt, no breaker;
- **retries**: jittered retries inside a deadline, no breaker;
- **retries + breaker**: the production configuration.

Measured per configuration: success rate, latency percentiles, calls reaching the
vehicle service per partner request (load amplification), and, for the outage,
how fast requests fail while the service is down and how soon after it recovers
requests succeed again.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx

from . import system
from .resilience.breaker import CircuitBreaker
from .resilience.retry import RetryPolicy
from .vehicles.service import Chaos

TIMEOUT_S = 0.25


@dataclass
class Sample:
    t: float  # seconds since start
    ok: bool
    status: int
    latency_ms: float


def _config(name: str):
    if name == "none":
        return dict(resilient=False)
    retry = RetryPolicy(attempts=3, base_s=0.02, cap_s=0.2, deadline_s=1.0)
    if name == "retries":
        # breaker that never opens: same code path, breaker effectively disabled
        return dict(resilient=True, retry=retry, breaker=CircuitBreaker("off", min_calls=10**9))
    return dict(resilient=True, retry=retry,
                breaker=CircuitBreaker("vehicle-service", failure_threshold=0.5, window=20, min_calls=10,
                                       open_s=0.5, half_open_trials=3))


async def _run(scenario: str, config: str, duration_s: float, workers: int, seed: int) -> dict:
    chaos = Chaos(failure_rate=0.3 if scenario == "flaky" else 0.0, seed=seed)
    s = await system.build(chaos=chaos, timeout_s=TIMEOUT_S, rate=(10_000, 10_000), webhook_url=None, seed=seed,
                           **_config(config))
    samples: list[Sample] = []
    vins = sorted(s.partner.vins)
    outage_calls = {}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(s.app), base_url="http://gw") as c:
            tok = (await c.post("/oauth/token", data={"grant_type": "client_credentials"},
                                auth=(s.partner.client_id, s.secret))).json()["access_token"]
            h = {"Authorization": f"Bearer {tok}"}
            start = time.perf_counter()
            calls_before = s.servicer.calls

            async def phase_control():
                if scenario != "outage":
                    return
                await asyncio.sleep(duration_s / 3)
                outage_calls["start"] = s.servicer.calls
                chaos.extra_latency_s = 2.0  # hang: every call outlives the client's deadline
                await asyncio.sleep(duration_s / 3)
                chaos.extra_latency_s = 0.0
                outage_calls["end"] = s.servicer.calls

            async def worker(i: int):
                n = 0
                while (now := time.perf_counter() - start) < duration_s:
                    t0 = time.perf_counter()
                    r = await c.get(f"/v1/vehicles/{vins[(i + n) % len(vins)]}", headers=h)
                    samples.append(Sample(now, r.status_code == 200, r.status_code, (time.perf_counter() - t0) * 1000))
                    n += 1
                    await asyncio.sleep(0.01)

            await asyncio.gather(phase_control(), *(worker(i) for i in range(workers)))
            upstream_calls = s.servicer.calls - calls_before
    finally:
        await s.close()
    out = _summarize(scenario, config, samples, upstream_calls, duration_s)
    if outage_calls:
        out["during_outage_upstream_calls"] = outage_calls["end"] - outage_calls["start"]
    return out


def _pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def _summarize(scenario, config, samples: list[Sample], upstream_calls: int, duration_s: float) -> dict:
    lat = [x.latency_ms for x in samples]
    out = {
        "scenario": scenario, "config": config, "requests": len(samples),
        "success_rate": round(sum(x.ok for x in samples) / len(samples), 4),
        "p50_ms": round(_pct(lat, 50), 1), "p95_ms": round(_pct(lat, 95), 1), "p99_ms": round(_pct(lat, 99), 1),
        "upstream_calls_per_request": round(upstream_calls / len(samples), 2),
        "status_counts": {str(k): sum(1 for x in samples if x.status == k) for k in sorted({x.status for x in samples})},
    }
    if scenario == "outage":
        a, b = duration_s / 3, 2 * duration_s / 3
        during = [x for x in samples if a + 0.1 <= x.t < b]
        after = [x for x in samples if x.t >= b]
        first_ok = min((x.t for x in after if x.ok), default=None)
        out.update({
            "during_outage_requests": len(during),
            "during_outage_median_ms": round(statistics.median(x.latency_ms for x in during), 1) if during else None,
            "recovery_s": round(first_ok - b, 3) if first_ok is not None else None,
            "after_recovery_success_rate": round(sum(x.ok for x in after) / len(after), 4) if after else None,
        })
    out["timeline"] = [asdict(x) for x in samples]
    return out


async def run_all(duration_s: float = 6.0, workers: int = 20, repeats: int = 3) -> list[dict]:
    """Each scenario/config runs `repeats` times (seeds 0..n-1). Numbers are medians across runs, with the
    min-max range kept for the noisiest one (success right after recovery); the chart uses the first run."""
    results = []
    for scenario, dur in (("flaky", duration_s / 2), ("outage", duration_s)):
        for config in ("none", "retries", "retries+breaker"):
            runs = [await _run(scenario, config, dur, workers, seed) for seed in range(repeats)]
            agg = dict(runs[0])
            for k, v in runs[0].items():
                if isinstance(v, (int, float)) and not isinstance(v, bool) and all(r.get(k) is not None for r in runs):
                    agg[k] = statistics.median(r[k] for r in runs)
            if scenario == "outage":
                vals = [r["after_recovery_success_rate"] for r in runs]
                agg["after_recovery_success_range"] = [min(vals), max(vals)]
            agg["repeats"] = repeats
            results.append(agg)
    return results


# -- report ---------------------------------------------------------------------------

def _f(v, spec: str, suffix: str = "") -> str:
    return "n/a" if v is None else f"{v:{spec}}{suffix}"


def write_report(results: list[dict], out: str | Path) -> Path:
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "chaos.json").write_text(json.dumps([{k: v for k, v in r.items() if k != "timeline"} for r in results],
                                               indent=2))
    _chart(results, out / "outage_timeline.png")
    names = {"none": "No retries, no breaker", "retries": "Retries only", "retries+breaker": "Retries + circuit breaker"}
    lines = ["# Chaos experiment", "",
             "Generated by `pgw chaos`. Everything runs in one process (real gRPC over mTLS, real gateway), "
             f"with 20 concurrent partner clients reading vehicle state. Client deadline per call: {TIMEOUT_S * 1000:.0f} ms. "
             f"Each row is the median of {results[0].get('repeats', 1)} runs.",
             "", "## Flaky: 30% of vehicle-service calls fail", "",
             "| Gateway | Success | p50 | p95 | Calls to vehicle service per request |", "|---|---:|---:|---:|---:|"]
    for r in results:
        if r["scenario"] == "flaky":
            lines.append(f"| {names[r['config']]} | {r['success_rate']:.1%} | {r['p50_ms']:.1f} ms | {r['p95_ms']:.1f} ms | "
                         f"{r['upstream_calls_per_request']:.2f} |")
    lines += ["", "## Outage: the vehicle service hangs for 2 s, then recovers", "",
              "Clients send their next request as soon as the previous one returns, so slow failures mean fewer "
              "requests. The comparison uses the outage window itself.", "",
              "| Gateway | Partner requests while down | Median response while down | Calls sent to the hung service "
              "| Success in the 2 s after recovery |", "|---|---:|---:|---:|---:|"]
    for r in results:
        if r["scenario"] == "outage":
            lo, hi = r.get("after_recovery_success_range") or [r["after_recovery_success_rate"]] * 2
            spread = f" ({lo:.0%}–{hi:.0%})" if None not in (lo, hi) and hi - lo >= 0.005 else ""
            lines.append(f"| {names[r['config']]} | {_f(r['during_outage_requests'], '.0f')} | "
                         f"{_f(r['during_outage_median_ms'], '.1f', ' ms')} | {_f(r.get('during_outage_upstream_calls'), '.0f')} | "
                         f"{_f(r['after_recovery_success_rate'], '.1%')}{spread} |")
    lines += ["", "![Outage timeline](outage_timeline.png)", ""]
    (out / "CHAOS.md").write_text("\n".join(lines))
    return out


def _chart(results: list[dict], path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    surface, ink, muted, grid = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
    colors = {"none": "#eb6834", "retries": "#eda100", "retries+breaker": "#2a78d6"}
    labels = {"none": "No retries, no breaker", "retries": "Retries only", "retries+breaker": "Retries + breaker"}
    outage = [r for r in results if r["scenario"] == "outage"]
    fig, ax = plt.subplots(figsize=(9, 4))
    fig.patch.set_facecolor(surface)
    ax.set_facecolor(surface)
    fig.subplots_adjust(left=0.09, right=0.97, top=0.74, bottom=0.14)
    fig.text(0.02, 0.95, "Response time during a vehicle-service outage", fontsize=13, fontweight="bold",
             color=ink, va="top")
    fig.text(0.02, 0.885, "Median response time of requests started in each 100 ms; the service hangs from 2 s to 4 s (shaded)",
             fontsize=9.5, color=muted, va="top")
    dur = max(x["t"] for r in outage for x in r["timeline"])
    ax.axvspan(dur / 3, 2 * dur / 3, color=grid, alpha=0.6, lw=0)
    for r in outage:
        buckets: dict[int, list[float]] = {}
        for x in r["timeline"]:
            buckets.setdefault(int(x["t"] * 10), []).append(x["latency_ms"])
        # one point per 100 ms in which requests started; no line across stretches with no new requests
        xs = range(int(dur * 10) + 1)
        ys = [statistics.median(buckets[b]) if b in buckets else float("nan") for b in xs]
        ax.plot([b / 10 for b in xs], ys, color=colors[r["config"]], lw=2, marker="o", ms=3,
                label=labels[r["config"]])
    ax.set_yscale("log")
    ax.set_xlabel("seconds", color=muted)
    ax.set_ylabel("ms (log scale)", color=muted)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.spines["left"].set_color(grid)
    ax.spines["bottom"].set_color(grid)
    ax.tick_params(colors=muted)
    ax.grid(axis="y", color=grid, lw=0.8)
    ax.legend(ncol=3, loc="lower left", bbox_to_anchor=(0, 1.02), frameon=False, fontsize=9, labelcolor=muted)
    fig.savefig(path, dpi=160, facecolor=surface)
    plt.close(fig)
