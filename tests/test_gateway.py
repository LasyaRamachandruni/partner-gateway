"""End to end: partner API -> gateway -> gRPC over mTLS -> fleet -> event -> webhook."""

import asyncio
import base64
import time

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from partner_sdk import verify_signature
from pgw import certs
from pgw.gateway.upstream import VehicleClient
from pgw.proto import vehicle_pb2 as pb
from pgw.proto import vehicle_pb2_grpc as rpc

from .conftest import SPANS, token


async def _wait(predicate, timeout=5.0):
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise TimeoutError


# OAuth -----------------------------------------------------------------------

async def test_token_endpoint(api, sys_):
    p, s = sys_.partner, sys_.secret
    ok = await api.post("/oauth/token", data={"grant_type": "client_credentials", "client_id": p.client_id,
                                              "client_secret": s, "scope": "vehicles:read"})
    assert ok.status_code == 200 and ok.json()["scope"] == "vehicles:read"
    assert ok.headers["cache-control"] == "no-store"
    basic = base64.b64encode(f"{p.client_id}:{s}".encode()).decode()
    assert (await api.post("/oauth/token", data={"grant_type": "client_credentials"},
                           headers={"Authorization": f"Basic {basic}"})).status_code == 200
    assert (await api.post("/oauth/token", data={"grant_type": "client_credentials"},
                           auth=(p.client_id, "wrong"))).json() == {"error": "invalid_client"}
    assert (await api.post("/oauth/token", data={"grant_type": "password"},
                           auth=(p.client_id, s))).status_code == 400
    jwks = (await api.get("/.well-known/jwks.json")).json()
    assert jwks["keys"][0]["kid"] == sys_.gateway.keys.active.kid


async def test_token_endpoint_slows_down_guessing(api, sys_):
    codes = [(await api.post("/oauth/token", data={"grant_type": "client_credentials"},
                             auth=(sys_.partner.client_id, "guess"))).status_code for _ in range(15)]
    assert codes[:10] == [401] * 10 and 429 in codes[10:]


async def test_requests_need_a_valid_token_and_scope(api, sys_):
    vin = sorted(sys_.partner.vins)[0]
    assert (await api.get(f"/v1/vehicles/{vin}")).status_code == 401
    assert (await api.get(f"/v1/vehicles/{vin}", headers={"Authorization": "Bearer nope"})).status_code == 401
    read_only = await token(api, sys_, "vehicles:read")
    r = await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "LOCK"},
                       headers={"Authorization": f"Bearer {read_only}"})
    assert r.status_code == 403 and r.json()["error"]["code"] == "insufficient_scope"
    assert r.json()["error"]["request_id"] == r.headers["x-request-id"]


# vehicles ----------------------------------------------------------------------

async def test_read_vehicle_state_over_mtls_grpc(api, auth, sys_):
    vins = (await api.get("/v1/vehicles", headers=auth)).json()["vehicles"]
    assert set(vins) == sys_.partner.vins
    r = await api.get(f"/v1/vehicles/{vins[0]}", headers=auth)
    assert r.status_code == 200
    body = r.json()
    car = sys_.fleet.get(vins[0])
    assert body["battery_percent"] == car.battery_percent and body["locked"] == car.locked
    assert int(r.headers["x-ratelimit-remaining"]) >= 0


async def test_vehicles_without_a_grant_are_invisible(api, auth, sys_):
    not_shared = next(v for v in sys_.fleet.vehicles if v not in sys_.partner.vins)
    r = await api.get(f"/v1/vehicles/{not_shared}", headers=auth)
    assert r.status_code == 404  # same answer as a VIN that doesn't exist
    assert (await api.get("/v1/vehicles/NOPE", headers=auth)).status_code == 404
    assert sys_.servicer.calls == 0  # rejected before reaching the vehicle service


async def test_command_lifecycle_with_signed_webhook(api, auth, sys_, receiver):
    vin = sorted(sys_.partner.vins)[0]
    sys_.fleet.get(vin).locked = True
    r = await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "UNLOCK"}, headers=auth)
    assert r.status_code == 202 and r.json()["status"] == "pending"
    cid = r.json()["command_id"]
    assert r.headers["location"] == f"/v1/commands/{cid}"
    await _wait(lambda: receiver.received)
    status = (await api.get(f"/v1/commands/{cid}", headers=auth)).json()
    assert status["status"] == "succeeded" and sys_.fleet.get(vin).locked is False

    hook = receiver.received[0]
    assert hook["event"]["type"] == "command.completed" and hook["event"]["data"]["command_id"] == cid
    verify_signature(hook["body"], hook["headers"]["pgw-signature"], sys_.partner.webhook_secrets)
    assert hook["headers"]["pgw-event-id"] == hook["event"]["id"]


async def test_failed_and_unknown_commands(api, auth, sys_, receiver):
    vin = next(v for v in sorted(sys_.partner.vins) if not sys_.fleet.get(v).plugged_in)
    cid = (await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "START_CHARGING"}, headers=auth)).json()["command_id"]
    await _wait(lambda: receiver.received)
    c = (await api.get(f"/v1/commands/{cid}", headers=auth)).json()
    assert c["status"] == "failed" and c["reason"] == "vehicle is not plugged in"
    bad = await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "SELF_DESTRUCT"}, headers=auth)
    assert bad.status_code == 422
    assert (await api.get("/v1/commands/cmd_nope", headers=auth)).status_code == 404


async def test_offline_vehicle_runs_command_on_reconnect(api, auth, sys_, receiver):
    vin = sorted(sys_.partner.vins)[1]
    sys_.fleet.set_online(vin, False)
    cid = (await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "HONK_AND_FLASH"}, headers=auth)).json()["command_id"]
    await asyncio.sleep(0.1)
    assert (await api.get(f"/v1/commands/{cid}", headers=auth)).json()["status"] == "pending"
    sys_.fleet.set_online(vin, True)
    await _wait(lambda: receiver.received)
    assert (await api.get(f"/v1/commands/{cid}", headers=auth)).json()["status"] == "succeeded"


async def test_idempotency_key_prevents_double_commands(api, auth, sys_):
    vin = sorted(sys_.partner.vins)[2]
    h = {**auth, "Idempotency-Key": "abc-123"}
    first = await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=h)
    again = await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=h)
    assert again.status_code == 202 and again.json()["command_id"] == first.json()["command_id"]
    assert again.headers["idempotent-replayed"] == "true"
    assert len(sys_.fleet.seen) == 1
    reused = await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "UNLOCK"}, headers=h)
    assert reused.status_code == 422 and reused.json()["error"]["code"] == "idempotency_key_reused"


async def test_rate_limit_returns_retry_after(api, sys_):
    sys_.partner.rate_per_s, sys_.partner.burst = 1.0, 3
    sys_.gateway._limiters.clear()
    h = {"Authorization": f"Bearer {await token(api, sys_)}"}
    codes = [(await api.get("/v1/vehicles", headers=h)) for _ in range(5)]
    assert [r.status_code for r in codes] == [200, 200, 200, 429, 429]
    assert codes[3].headers["retry-after"] == "1" and codes[3].json()["error"]["code"] == "rate_limited"


# webhooks -------------------------------------------------------------------------

async def test_webhook_retries_then_succeeds(api, auth, sys_, receiver):
    receiver.fail_next = 2
    await api.post("/v1/webhooks/test", headers=auth)
    await _wait(lambda: receiver.received)
    d = next(iter(sys_.gateway.webhooks.deliveries.values()))
    assert d.status == "delivered" and d.attempts == 3


async def test_webhook_dead_letter_and_replay(api, auth, sys_, receiver):
    receiver.fail_next = 10
    eid = (await api.post("/v1/webhooks/test", headers=auth)).json()["event_id"]
    await sys_.gateway.webhooks.drain()
    dead = (await api.get("/v1/webhooks/deliveries?status=dead", headers=auth)).json()["deliveries"]
    assert [d["event_id"] for d in dead] == [eid] and dead[0]["attempts"] == 4
    receiver.fail_next = 0
    assert (await api.post(f"/v1/webhooks/deliveries/{eid}/replay", headers=auth)).status_code == 202
    await _wait(lambda: receiver.received)
    assert receiver.received[0]["event"]["id"] == eid


async def test_webhook_4xx_is_not_retried(api, auth, sys_, receiver):
    receiver.fail_next, receiver.status_on_fail = 1, 400
    await api.post("/v1/webhooks/test", headers=auth)
    await sys_.gateway.webhooks.drain()
    d = next(iter(sys_.gateway.webhooks.deliveries.values()))
    assert d.status == "dead" and d.attempts == 1


async def test_webhook_secret_rotation_signs_with_both(api, auth, sys_, receiver):
    old = sys_.partner.webhook_secrets[0]
    new = sys_.gateway.partners.rotate_webhook_secret(sys_.partner.partner_id)
    await api.post("/v1/webhooks/test", headers=auth)
    await _wait(lambda: receiver.received)
    h = receiver.received[0]
    assert h["headers"]["pgw-signature"].count("v1=") == 2
    verify_signature(h["body"], h["headers"]["pgw-signature"], old)
    verify_signature(h["body"], h["headers"]["pgw-signature"], new)


# events: at-least-once ------------------------------------------------------------

async def test_duplicate_result_events_are_ignored(api, auth, sys_, receiver):
    vin = sorted(sys_.partner.vins)[3]
    cid = (await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=auth)).json()["command_id"]
    await _wait(lambda: receiver.received)
    await sys_.bus.publish("vehicle-command-results", {"command_id": cid, "status": "FAILED", "reason": "late dup"})
    await sys_.bus.drain()
    assert (await api.get(f"/v1/commands/{cid}", headers=auth)).json()["status"] == "succeeded"
    assert sys_.gateway.commands.duplicates_ignored == 1 and len(receiver.received) == 1


async def test_lost_result_expires_command(api, auth, sys_):
    vin = sorted(sys_.partner.vins)[4]
    sys_.fleet.set_online(vin, False)
    cid = (await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=auth)).json()["command_id"]
    assert sys_.gateway.commands.sweep(now=time.time() + 1000) == 1
    assert (await api.get(f"/v1/commands/{cid}", headers=auth)).json()["status"] == "expired"


# security of the internal hop ---------------------------------------------------------

async def test_vehicle_service_rejects_clients_without_our_certificates(sys_):
    vin = next(iter(sys_.fleet.vehicles))
    # 1. no TLS at all
    plain = VehicleClient(f"127.0.0.1:{sys_.port}", resilient=False, timeout_s=0.5)
    with pytest.raises(Exception):
        await plain.get_state(vin)
    await plain.close()
    # 2. a certificate from a different CA
    other = certs.generate()
    rogue = VehicleClient(f"127.0.0.1:{sys_.port}", resilient=False, timeout_s=0.5, server_name="vehicle-service",
                          tls=(sys_.bundle.ca.cert_pem, other.client.cert_pem, other.client.key_pem))
    with pytest.raises(Exception):
        await rogue.get_state(vin)
    await rogue.close()
    # 3. a valid certificate from our CA, but an identity that isn't allowed
    bundle = sys_.bundle
    real_ca = x509.load_pem_x509_certificate(bundle.ca.cert_pem)
    real_key = serialization.load_pem_private_key(bundle.ca.key_pem, None)
    billing = certs.issue(real_ca, real_key, "billing-service", server=False)
    creds = grpc.ssl_channel_credentials(bundle.ca.cert_pem, billing.key_pem, billing.cert_pem)
    async with grpc.aio.secure_channel(f"127.0.0.1:{sys_.port}", creds,
                                       options=[("grpc.ssl_target_name_override", "vehicle-service")]) as ch:
        with pytest.raises(grpc.aio.AioRpcError) as e:
            await rpc.VehicleServiceStub(ch).GetVehicleState(pb.GetVehicleStateRequest(vin=vin), timeout=1)
        assert e.value.code() == grpc.StatusCode.PERMISSION_DENIED


# observability ---------------------------------------------------------------------

async def test_one_trace_from_request_to_webhook(api, auth, sys_, receiver):
    SPANS.clear()
    vin = sorted(sys_.partner.vins)[0]
    cid = (await api.post(f"/v1/vehicles/{vin}/commands", json={"type": "LOCK"}, headers=auth)).json()["command_id"]
    await _wait(lambda: receiver.received)
    await sys_.gateway.webhooks.drain()
    spans = SPANS.get_finished_spans()
    root = next(s for s in spans if s.name == "POST /v1/vehicles/{vin}/commands")
    trace = [s for s in spans if s.context.trace_id == root.context.trace_id]
    names = {s.name for s in trace}
    assert {"VehicleService/SendCommand", "vehicle.command.result", "command.result", "webhook.deliver"} <= names
    tp = receiver.received[0]["headers"]["traceparent"]
    assert tp.split("-")[1] == format(root.context.trace_id, "032x")


async def test_metrics_and_health(api, auth, sys_):
    vin = sorted(sys_.partner.vins)[0]
    await api.get(f"/v1/vehicles/{vin}", headers=auth)
    text = (await api.get("/metrics")).text
    assert 'pgw_http_requests_total{method="GET",route="/v1/vehicles/{vin}",status="200"} 1.0' in text
    assert 'pgw_upstream_calls_total{method="GetVehicleState",outcome="OK"} 1.0' in text
    assert vin not in text  # route templates, not raw paths
    assert (await api.get("/healthz")).json() == {"status": "ok"}
    assert (await api.get("/readyz")).json()["ready"] is True
