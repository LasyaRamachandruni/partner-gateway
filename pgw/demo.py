"""The sandbox's stand-in for a partner's webhook endpoint: verifies each delivery's signature and prints it."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from partner_sdk import InvalidSignature, verify_signature


def webhook_receiver(secrets: list[str]) -> FastAPI:
    app = FastAPI()

    @app.post("/hooks")
    async def hooks(request: Request):
        body = await request.body()
        try:
            verify_signature(body, request.headers.get("pgw-signature", ""), secrets)
        except InvalidSignature as e:
            print(f"webhook REJECTED: {e}", flush=True)
            return JSONResponse({"error": str(e)}, 400)
        print(f"webhook received (signature verified): {body.decode()}", flush=True)
        return {"ok": True}

    return app
