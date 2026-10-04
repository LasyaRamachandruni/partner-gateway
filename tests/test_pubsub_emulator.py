"""The Google Cloud Pub/Sub bus against the Pub/Sub emulator.

Runs only when PUBSUB_EMULATOR_HOST is set (CI starts the emulator for this job):

    docker run -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \\
        gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
    PUBSUB_EMULATOR_HOST=localhost:8085 pytest tests/test_pubsub_emulator.py
"""

import asyncio
import os
import uuid

import httpx
import pytest

from partner_sdk import PartnerClient

pytestmark = pytest.mark.skipif(not os.environ.get("PUBSUB_EMULATOR_HOST"),
                                reason="needs the Pub/Sub emulator (PUBSUB_EMULATOR_HOST)")


async def test_publish_and_subscribe_through_the_emulator():
    from pgw.bus.gcp import GcpPubSubBus

    bus = GcpPubSubBus(f"pgw-test-{uuid.uuid4().hex[:6]}", create=True)
    got = []

    async def handler(m):
        got.append((m.data, m.attributes))

    await bus.subscribe("t1", "t1-sub", handler)
    await bus.publish("t1", {"hello": "world"}, {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"})
    for _ in range(200):
        if got:
            break
        await asyncio.sleep(0.05)
    await bus.close()
    assert got[0][0] == {"hello": "world"} and got[0][1]["traceparent"].startswith("00-111")


async def test_full_system_over_pubsub(receiver):
    from pgw import system
    from pgw.bus.gcp import GcpPubSubBus

    bus = GcpPubSubBus(f"pgw-test-{uuid.uuid4().hex[:6]}", create=True)
    s = await system.build(bus=bus, webhook_transport=httpx.ASGITransport(receiver.app))
    try:
        async with PartnerClient("http://gw.test", s.partner.client_id, s.secret,
                                 transport=httpx.ASGITransport(s.app)) as pc:
            vin = sorted(s.partner.vins)[0]
            cmd = await pc.send_command(vin, "LOCK")
            done = await pc.wait_for_command(cmd["command_id"], timeout_s=20, poll_s=0.1)
        assert done["status"] == "succeeded"
        for _ in range(100):
            if receiver.received:
                break
            await asyncio.sleep(0.05)
        assert receiver.received[0]["event"]["data"]["command_id"] == cmd["command_id"]
    finally:
        await s.close()
