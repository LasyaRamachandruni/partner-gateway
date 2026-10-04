"""Partner SDK client.

    async with PartnerClient("https://gateway.example", client_id, client_secret) as pc:
        state = await pc.get_vehicle("1FT...")
        cmd = await pc.send_command("1FT...", "UNLOCK")         # safe to retry: has an idempotency key
        done = await pc.wait_for_command(cmd["command_id"])

What it handles so partners don't have to:
- **Tokens.** Fetched with client credentials, cached, refreshed a minute before
  expiry, and refreshed once more if the gateway answers 401.
- **Retries.** On 429 and 503 it waits as long as the gateway's Retry-After says.
  On other 5xx and network errors it backs off with jitter. 4xx errors are raised immediately.
- **Idempotency.** Every `send_command` gets a fresh Idempotency-Key, reused
  across that call's retries. A retry after a lost response can't send the
  command twice, and the partner gets back the original response.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid

import httpx


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, request_id: str | None = None):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message, self.request_id = status, code, message, request_id


class PartnerClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str, *, scope: str | None = None,
                 max_attempts: int = 5, transport: httpx.AsyncBaseTransport | None = None,
                 sleep=asyncio.sleep, timeout_s: float = 10.0):
        self.http = httpx.AsyncClient(base_url=base_url, transport=transport, timeout=timeout_s)
        self.client_id, self.client_secret, self.scope = client_id, client_secret, scope
        self.max_attempts = max_attempts
        self.sleep = sleep
        self._token: str | None = None
        self._token_expires = 0.0
        self.token_fetches = 0
        self.retries = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.http.aclose()

    async def _fetch_token(self) -> str:
        data = {"grant_type": "client_credentials"}
        if self.scope:
            data["scope"] = self.scope
        r = await self.http.post("/oauth/token", data=data, auth=(self.client_id, self.client_secret))
        if r.status_code != 200:
            raise ApiError(r.status_code, r.json().get("error", "token_error"), "could not get an access token")
        body = r.json()
        self.token_fetches += 1
        self._token = body["access_token"]
        self._token_expires = time.time() + body["expires_in"]
        return self._token

    async def _auth(self, force: bool = False) -> str:
        if force or not self._token or time.time() > self._token_expires - 60:
            return await self._fetch_token()
        return self._token

    async def request(self, method: str, path: str, *, json: dict | None = None,
                      headers: dict | None = None) -> dict:
        refreshed = False
        for attempt in range(1, self.max_attempts + 1):
            token = await self._auth()
            try:
                r = await self.http.request(method, path, json=json,
                                            headers={"Authorization": f"Bearer {token}", **(headers or {})})
            except httpx.TransportError:
                if attempt == self.max_attempts:
                    raise
                self.retries += 1
                await self.sleep(random.uniform(0, min(8, 0.25 * 2 ** attempt)))
                continue
            if r.status_code == 401 and not refreshed:
                refreshed = True
                await self._auth(force=True)
                continue
            if r.status_code in (409, 429, 503) or r.status_code >= 500:
                if attempt == self.max_attempts:
                    break
                self.retries += 1
                retry_after = r.headers.get("Retry-After")
                await self.sleep(float(retry_after) if retry_after else random.uniform(0, 0.25 * 2 ** attempt))
                continue
            if r.status_code >= 400:
                break
            return r.json()
        err = r.json().get("error", {}) if r.headers.get("content-type", "").startswith("application/json") else {}
        raise ApiError(r.status_code, err.get("code", "error"), err.get("message", r.text), err.get("request_id"))

    async def list_vehicles(self) -> list[str]:
        return (await self.request("GET", "/v1/vehicles"))["vehicles"]

    async def get_vehicle(self, vin: str) -> dict:
        return await self.request("GET", f"/v1/vehicles/{vin}")

    async def send_command(self, vin: str, type_: str, idempotency_key: str | None = None) -> dict:
        key = idempotency_key or str(uuid.uuid4())
        return await self.request("POST", f"/v1/vehicles/{vin}/commands", json={"type": type_},
                                  headers={"Idempotency-Key": key})

    async def get_command(self, command_id: str) -> dict:
        return await self.request("GET", f"/v1/commands/{command_id}")

    async def wait_for_command(self, command_id: str, timeout_s: float = 60.0, poll_s: float = 0.5) -> dict:
        """Poll until the command is final. (Webhooks are the push alternative.)"""
        end = time.monotonic() + timeout_s
        while True:
            c = await self.get_command(command_id)
            if c["status"] != "pending" or time.monotonic() > end:
                return c
            await asyncio.sleep(poll_s)
