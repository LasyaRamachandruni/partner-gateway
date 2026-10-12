// Load test for the partner API, with thresholds taken from docs/SLO.md.
//
//   k6 run -e BASE_URLS=http://localhost:8080,http://localhost:8081 \
//          -e CLIENT_ID=... -e CLIENT_SECRET=... -e RATE=1000 -e DURATION=5m loadtest/gateway.js
//
// Traffic: 80% GET /v1/vehicles/{vin}, 15% GET /v1/vehicles, 5% POST a command with an
// Idempotency-Key. Requests are spread round-robin over BASE_URLS (one URL per replica locally,
// or the single Cloud Run URL). The partner used must have a rate plan above RATE
// (`pgw partners create --rate 2000 --burst 4000`), or the run measures the rate limiter.
//
// SLO thresholds (docs/SLO.md):
//   availability  share of requests answered without a 5xx      >= 99.9%   -> http_req_failed rate < 0.001
//   latency       GET /v1/vehicles/{vin} within 300 ms           >= 99%     -> p(99) < 300 ms
// plus: the generator actually reached the target rate (no more than 1% of iterations dropped).
//
// Access tokens live 15 minutes and are fetched once in setup(), so keep DURATION under 14m.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import exec from 'k6/execution';

const BASE_URLS = (__ENV.BASE_URLS || 'http://localhost:8080').split(',').map((u) => u.trim().replace(/\/$/, ''));
const RATE = parseInt(__ENV.RATE || '1000', 10);
const DURATION = __ENV.DURATION || '2m';
const WARMUP = __ENV.WARMUP || '15s'; // opens connection pools and channels; not judged by the thresholds
const SUMMARY_PATH = __ENV.SUMMARY_PATH || 'k6-summary.json';
const COMMANDS = ['LOCK', 'UNLOCK', 'HONK_AND_FLASH'];

const rateLimited = new Rate('rate_limited');
const serverErrors = new Counter('server_errors');

// 4xx are the partner's errors, not ours: only 5xx and transport failures count against availability.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 499 }));

export const options = {
  discardResponseBodies: false,
  scenarios: {
    warmup: {
      executor: 'constant-arrival-rate',
      rate: Math.max(1, Math.ceil(RATE / 10)),
      timeUnit: '1s',
      duration: WARMUP,
      preAllocatedVUs: 20,
      maxVUs: 200,
    },
    partner_api: {
      executor: 'constant-arrival-rate',
      startTime: WARMUP,
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: parseInt(__ENV.PRE_VUS || String(Math.max(50, Math.ceil(RATE / 4))), 10),
      maxVUs: parseInt(__ENV.MAX_VUS || String(Math.max(200, RATE * 2)), 10),
    },
  },
  thresholds: {
    'http_req_failed{scenario:partner_api}': ['rate<0.001'],
    'http_req_duration{scenario:partner_api,name:vehicle_state}': ['p(99)<300'],
    'rate_limited{scenario:partner_api}': ['rate<0.01'],
    'dropped_iterations{scenario:partner_api}': [`count<${Math.ceil(RATE * durationSeconds(DURATION) * 0.01)}`],
    // always true: these only make k6 report the measured scenario on its own in the summary
    'http_reqs{scenario:partner_api}': ['count>0'],
    'http_req_duration{scenario:partner_api}': ['max>=0'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function durationSeconds(d) {
  const m = /^(\d+)(ms|s|m|h)$/.exec(d);
  if (!m) return 60;
  const n = parseInt(m[1], 10);
  return { ms: n / 1000, s: n, m: n * 60, h: n * 3600 }[m[2]];
}

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export function setup() {
  if (!__ENV.CLIENT_ID || !__ENV.CLIENT_SECRET) {
    throw new Error('set CLIENT_ID and CLIENT_SECRET (pgw partners create prints them)');
  }
  const res = http.post(`${BASE_URLS[0]}/oauth/token`, {
    grant_type: 'client_credentials',
    client_id: __ENV.CLIENT_ID,
    client_secret: __ENV.CLIENT_SECRET,
  });
  if (res.status !== 200) throw new Error(`token request failed: ${res.status} ${res.body}`);
  const token = res.json('access_token');
  const vehicles = http.get(`${BASE_URLS[0]}/v1/vehicles`, { headers: { Authorization: `Bearer ${token}` } });
  const vins = vehicles.json('vehicles');
  if (!vins || vins.length === 0) throw new Error('the partner has no vehicles');
  return { token, vins };
}

export default function (data) {
  const base = BASE_URLS[exec.scenario.iterationInTest % BASE_URLS.length];
  const headers = { Authorization: `Bearer ${data.token}` };
  const vin = data.vins[Math.floor(Math.random() * data.vins.length)];
  const roll = Math.random();
  let res;
  if (roll < 0.8) {
    res = http.get(`${base}/v1/vehicles/${vin}`, { headers, tags: { name: 'vehicle_state' } });
    check(res, { 'state 200': (r) => r.status === 200 });
  } else if (roll < 0.95) {
    res = http.get(`${base}/v1/vehicles`, { headers, tags: { name: 'list_vehicles' } });
    check(res, { 'list 200': (r) => r.status === 200 });
  } else {
    const type = COMMANDS[Math.floor(Math.random() * COMMANDS.length)];
    res = http.post(`${base}/v1/vehicles/${vin}/commands`, JSON.stringify({ type }), {
      headers: { ...headers, 'Content-Type': 'application/json', 'Idempotency-Key': uuid() },
      tags: { name: 'send_command' },
    });
    check(res, { 'command 202': (r) => r.status === 202 });
  }
  rateLimited.add(res.status === 429);
  if (res.status >= 500 || res.status === 0) serverErrors.add(1);
}

export function handleSummary(data) {
  const m = data.metrics;
  const v = (name, stat) => (m[name] && m[name].values ? m[name].values[stat] : null);
  // Everything below is the measured (post-warm-up) scenario only.
  const S = '{scenario:partner_api}';
  const requests = v(`http_reqs${S}`, 'count');
  const out = {
    target_rps: RATE,
    duration: DURATION,
    warmup: WARMUP,
    base_urls: BASE_URLS.length,
    achieved_rps: requests == null ? null : requests / durationSeconds(DURATION),
    requests,
    failed_rate: v(`http_req_failed${S}`, 'rate'),
    server_errors_including_warmup: v('server_errors', 'count') || 0,
    rate_limited_rate: v(`rate_limited${S}`, 'rate'),
    dropped_iterations: v(`dropped_iterations${S}`, 'count') || 0,
    latency_ms: {
      all: { p50: v(`http_req_duration${S}`, 'med'), p95: v(`http_req_duration${S}`, 'p(95)'), p99: v(`http_req_duration${S}`, 'p(99)') },
      vehicle_state: {
        p50: v('http_req_duration{scenario:partner_api,name:vehicle_state}', 'med'),
        p95: v('http_req_duration{scenario:partner_api,name:vehicle_state}', 'p(95)'),
        p99: v('http_req_duration{scenario:partner_api,name:vehicle_state}', 'p(99)'),
      },
    },
    thresholds: Object.fromEntries(
      Object.entries(m)
        .filter(([, x]) => x.thresholds)
        .map(([k, x]) => [k, Object.fromEntries(Object.entries(x.thresholds).map(([t, r]) => [t, r.ok]))]),
    ),
  };
  const lines = [
    `target ${RATE} rps for ${DURATION} (after ${WARMUP} warm-up) over ${BASE_URLS.length} URL(s)`,
    `achieved ${out.achieved_rps && out.achieved_rps.toFixed(1)} rps, ${out.requests} requests, ${out.dropped_iterations} dropped`,
    `5xx/transport failures ${(100 * (out.failed_rate || 0)).toFixed(3)}%, rate limited ${(100 * (out.rate_limited_rate || 0)).toFixed(3)}%`,
    `latency all p50/p95/p99 ${fmt(out.latency_ms.all)} ms; vehicle_state ${fmt(out.latency_ms.vehicle_state)} ms`,
    `thresholds: ${JSON.stringify(out.thresholds)}`,
  ];
  return { stdout: lines.join('\n') + '\n', [SUMMARY_PATH]: JSON.stringify(out, null, 2) };
}

function fmt(x) {
  return [x.p50, x.p95, x.p99].map((n) => (n == null ? 'n/a' : n.toFixed(1))).join('/');
}
