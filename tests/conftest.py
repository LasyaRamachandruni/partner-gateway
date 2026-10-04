import json

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import Response
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from pgw import system
from pgw.obs import tracing

SPANS = InMemorySpanExporter()
tracing.setup("test", SPANS)


class Receiver:
    """A partner's webhook endpoint. `fail_next` makes it answer 503 that many times."""

    def __init__(self):
        self.app = FastAPI()
        self.received: list[dict] = []
        self.fail_next = 0
        self.status_on_fail = 503

        @self.app.post("/hooks")
        async def hooks(request: Request):
            body = await request.body()
            if self.fail_next > 0:
                self.fail_next -= 1
                return Response(status_code=self.status_on_fail)
            self.received.append({"body": body, "headers": dict(request.headers), "event": json.loads(body)})
            return {"ok": True}


@pytest.fixture()
def receiver():
    return Receiver()


@pytest.fixture()
async def sys_(receiver):
    s = await system.build(webhook_transport=httpx.ASGITransport(receiver.app))
    yield s
    await s.close()


@pytest.fixture()
async def api(sys_):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(sys_.app), base_url="http://gw.test") as c:
        yield c


async def token(api, s, scope=None):
    data = {"grant_type": "client_credentials"}
    if scope:
        data["scope"] = scope
    r = await api.post("/oauth/token", data=data, auth=(s.partner.client_id, s.secret))
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.fixture()
async def auth(api, sys_):
    return {"Authorization": f"Bearer {await token(api, sys_)}"}
