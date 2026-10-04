"""The `pgw` command.

    pgw sandbox                       everything in one process + a demo partner; prints credentials
    pgw chaos --out results/          the chaos experiment (see pgw/chaos.py)
    pgw certs --out certs/            internal CA + service certificates for mTLS

    # as separate services (docker-compose runs these, with the Pub/Sub emulator as the bus):
    pgw vehicle-service --port 50051 --certs certs/ --vehicles 50
    pgw gateway --port 8080 --vehicle-service localhost:50051 --certs certs/ --demo-partner
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def _bus(name: str):
    if name == "pubsub":
        from .bus.gcp import GcpPubSubBus

        return GcpPubSubBus(os.environ.get("PUBSUB_PROJECT", "pgw-local"),
                            create=bool(os.environ.get("PUBSUB_EMULATOR_HOST")),
                            suffix=os.environ.get("PUBSUB_SUFFIX", ""))  # e.g. "-dev" to match Terraform
    from .bus.memory import InMemoryBus

    return InMemoryBus()


def cmd_certs(args) -> int:
    from . import certs

    out = certs.write(certs.generate(), args.out)
    print(f"wrote CA and certificates to {out}/")
    return 0


def cmd_chaos(args) -> int:
    from .chaos import run_all, write_report

    results = asyncio.run(run_all(duration_s=args.duration, workers=args.workers, repeats=args.repeats))
    out = write_report(results, args.out)
    print((out / "CHAOS.md").read_text())
    return 0


def cmd_vehicle_service(args) -> int:
    from . import certs
    from .vehicles.fleet import Fleet
    from .vehicles.service import serve

    from .obs import tracing

    tracing.setup_from_env("vehicle-service")

    async def main():
        bundle = certs.load(args.certs)
        bus = _bus(args.bus)
        fleet = Fleet(bus, [f"1FTVW1EL0{i:08d}" for i in range(args.vehicles)], time_scale=args.time_scale)
        server, port, _ = await serve(fleet, args.port, host=args.host, allowed_clients={"partner-gateway"},
                                      tls=(bundle.ca.cert_pem, bundle.server.cert_pem, bundle.server.key_pem))
        print(f"vehicle service: {args.vehicles} vehicles, gRPC + mTLS on {args.host}:{port}", flush=True)
        try:
            while True:
                await asyncio.sleep(30)
                await fleet.publish_telemetry()
        finally:
            await server.stop(grace=5)
            await bus.close()

    asyncio.run(main())
    return 0


def cmd_gateway(args) -> int:
    import httpx
    import uvicorn

    from . import certs
    from .auth.partners import PartnerRegistry
    from .gateway.app import Gateway, create_app
    from .gateway.upstream import VehicleClient
    from .gateway.webhooks import WebhookDispatcher

    from .obs import tracing

    tracing.setup_from_env("partner-gateway")
    bundle = certs.load(args.certs)
    registry = PartnerRegistry()
    if args.demo_partner:
        vins = {f"1FTVW1EL0{i:08d}" for i in range(args.vehicles)}
        p, secret = registry.register("Demo partner", {"vehicles:read", "vehicles:command"}, vins,
                                      webhook_url=args.webhook_url)
        print(json.dumps({"client_id": p.client_id, "client_secret": secret,
                          "webhook_secret": (p.webhook_secrets or [None])[0]}), flush=True)
    client = VehicleClient(args.vehicle_service, server_name="vehicle-service",
                           tls=(bundle.ca.cert_pem, bundle.client.cert_pem, bundle.client.key_pem))
    gw = Gateway(registry, client, _bus(args.bus), WebhookDispatcher(httpx.AsyncClient()))
    uvicorn.run(create_app(gw), host=args.host, port=args.port, log_level="info")
    return 0


def cmd_sandbox(args) -> int:
    import uvicorn

    from . import system
    from .demo import webhook_receiver

    async def main():
        s = await system.build(vehicles=args.vehicles, time_scale=0.2,
                               webhook_url=f"http://127.0.0.1:{args.port}/partner/hooks")
        s.app.mount("/partner", webhook_receiver(s.partner.webhook_secrets))  # the demo partner's endpoint
        print(json.dumps({"api": f"http://127.0.0.1:{args.port}", "client_id": s.partner.client_id,
                          "client_secret": s.secret, "vehicles": sorted(s.partner.vins)[:3]}, indent=2), flush=True)
        print("\ntry:\n  TOKEN=$(curl -s -u $CLIENT_ID:$CLIENT_SECRET -d grant_type=client_credentials "
              f"http://127.0.0.1:{args.port}/oauth/token | jq -r .access_token)\n"
              f"  curl -H \"Authorization: Bearer $TOKEN\" http://127.0.0.1:{args.port}/v1/vehicles\n", flush=True)
        server = uvicorn.Server(uvicorn.Config(s.app, host="127.0.0.1", port=args.port, log_level="warning"))
        try:
            await server.serve()
        finally:
            await s.close()

    asyncio.run(main())
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pgw", description="Partner gateway for connected vehicles")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("certs", help="generate the internal CA and mTLS certificates")
    c.add_argument("--out", default="certs")
    c.set_defaults(fn=cmd_certs)

    ch = sub.add_parser("chaos", help="run the chaos experiment and write a report")
    ch.add_argument("--out", default="results")
    ch.add_argument("--duration", type=float, default=6.0)
    ch.add_argument("--workers", type=int, default=20)
    ch.add_argument("--repeats", type=int, default=3)
    ch.set_defaults(fn=cmd_chaos)

    def common(x):
        x.add_argument("--certs", default="certs")
        x.add_argument("--bus", choices=["memory", "pubsub"], default="pubsub")
        x.add_argument("--host", default="0.0.0.0")
        x.add_argument("--vehicles", type=int, default=50)

    v = sub.add_parser("vehicle-service", help="run the simulated vehicle service (gRPC + mTLS)")
    common(v)
    v.add_argument("--port", type=int, default=50051)
    v.add_argument("--time-scale", type=float, default=1.0)
    v.set_defaults(fn=cmd_vehicle_service)

    g = sub.add_parser("gateway", help="run the partner gateway")
    common(g)
    g.add_argument("--port", type=int, default=8080)
    g.add_argument("--vehicle-service", default="localhost:50051")
    g.add_argument("--demo-partner", action="store_true", help="register a demo partner and print its credentials")
    g.add_argument("--webhook-url", default=None)
    g.set_defaults(fn=cmd_gateway)

    sb = sub.add_parser("sandbox", help="run everything in one process with a demo partner")
    sb.add_argument("--port", type=int, default=8080)
    sb.add_argument("--vehicles", type=int, default=10)
    sb.set_defaults(fn=cmd_sandbox)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
