"""The `pgw` command.

pgw sandbox                       everything in one process + a demo partner; prints credentials
pgw chaos --out results/          the chaos experiment (see pgw/chaos.py)
pgw certs --out certs/            internal CA + service certificates for mTLS

# the deployable services, configured by PGW_* environment variables (pgw/settings.py);
# flags override the environment:
pgw vehicle-service --port 50051
pgw gateway --port 8080

# operations
pgw db upgrade                    apply database migrations (PGW_DATABASE_URL)
pgw partners create --name "Acme Insurance" --demo-vins 50 --rate 1000 --burst 2000
pgw keys init --out keyset.json   signing key set; then: keys stage | activate | prune
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger("pgw")

Command = Callable[[argparse.Namespace], int]


def _settings(**overrides: Any) -> Any:
    from .settings import Settings

    return Settings(**{k: v for k, v in overrides.items() if v is not None})


def _setup_obs(service: str, settings: Any) -> None:
    from .obs import logging as obs_logging
    from .obs import tracing

    obs_logging.setup(service, settings.log_level, settings.log_format)
    tracing.setup_from_env(service)


def _out(obj: Any) -> None:
    """Command output meant for the operator (credentials, key ids) goes to stdout as JSON."""
    sys.stdout.write(json.dumps(obj) + "\n")  # one line: one log entry when run as a Cloud Run job
    sys.stdout.flush()


# -- local tools ------------------------------------------------------------------------


def cmd_certs(args: argparse.Namespace) -> int:
    from . import certs

    out = certs.write(certs.generate(), args.out)
    _out({"certs": str(out)})
    return 0


def cmd_chaos(args: argparse.Namespace) -> int:
    from .chaos import run_all, write_report

    results = asyncio.run(run_all(duration_s=args.duration, workers=args.workers, repeats=args.repeats))
    out = write_report(results, args.out)
    sys.stdout.write((out / "CHAOS.md").read_text())
    return 0


def cmd_sandbox(args: argparse.Namespace) -> int:
    import uvicorn

    from . import system
    from .demo import webhook_receiver

    async def main() -> None:
        s = await system.build(
            vehicles=args.vehicles, time_scale=0.2, webhook_url=f"http://127.0.0.1:{args.port}/partner/hooks"
        )
        s.app.mount("/partner", webhook_receiver(s.partner.webhook_secrets))  # the demo partner's endpoint
        _out(
            {
                "api": f"http://127.0.0.1:{args.port}",
                "client_id": s.partner.client_id,
                "client_secret": s.secret,
                "vehicles": sorted(s.partner.vins)[:3],
            }
        )
        sys.stdout.write(
            "\ntry:\n  TOKEN=$(curl -s -u $CLIENT_ID:$CLIENT_SECRET -d grant_type=client_credentials "
            f"http://127.0.0.1:{args.port}/oauth/token | jq -r .access_token)\n"
            f'  curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:{args.port}/v1/vehicles\n\n'
        )
        server = uvicorn.Server(uvicorn.Config(s.app, host="127.0.0.1", port=args.port, log_level="warning"))
        try:
            await server.serve()
        finally:
            await s.close()

    asyncio.run(main())
    return 0


# -- services ---------------------------------------------------------------------------


def cmd_vehicle_service(args: argparse.Namespace) -> int:
    from . import certs
    from .runtime import build_backend, open_bus
    from .vehicles.service import serve

    settings = _settings(
        port=args.port,
        certs_dir=args.certs,
        bus=args.bus,
        host=args.host,
        vehicles=args.vehicles,
        time_scale=args.time_scale,
        grpc_tls=args.tls,
        vehicle_backend=args.backend,
    )
    _setup_obs("vehicle-service", settings)

    async def main() -> None:
        bus = open_bus(settings)
        backend = await build_backend(settings, bus)
        tls = None
        allowed: set[str] | None = None
        if settings.grpc_tls == "mtls":
            bundle = certs.load(settings.certs_dir)
            tls = (bundle.ca.cert_pem, bundle.server.cert_pem, bundle.server.key_pem)
            allowed = {"partner-gateway"}
        server, port, _ = await serve(backend, settings.port, host=settings.host, allowed_clients=allowed, tls=tls)
        log.info("vehicle service listening", extra={"port": port, "backend": backend.name, "tls": settings.grpc_tls})
        try:
            while True:
                await asyncio.sleep(30)
                await backend.publish_telemetry()
        finally:
            await server.stop(grace=5)
            await backend.close()
            await bus.close()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    return 0


def cmd_gateway(args: argparse.Namespace) -> int:
    import uvicorn

    from .gateway.app import create_app
    from .runtime import build_gateway, demo_vins

    settings = _settings(
        port=args.port, certs_dir=args.certs, bus=args.bus, host=args.host, vehicle_service=args.vehicle_service
    )
    _setup_obs("partner-gateway", settings)

    async def main() -> None:
        rt = await build_gateway(settings)
        try:
            if args.demo_partner:
                p, secret = await rt.gateway.partners.register(
                    "Demo partner",
                    {"vehicles:read", "vehicles:command"},
                    set(demo_vins(args.vehicles or settings.vehicles)),
                    webhook_url=args.webhook_url,
                )
                _out(
                    {
                        "client_id": p.client_id,
                        "client_secret": secret,
                        "webhook_secret": p.webhook_secrets[0] if p.webhook_secrets else None,
                    }
                )
            config = uvicorn.Config(
                create_app(rt.gateway),
                host=settings.host,
                port=settings.port,
                log_config=None,
                access_log=False,
                timeout_graceful_shutdown=10,
            )
            await uvicorn.Server(config).serve()
        finally:
            await rt.close()

    asyncio.run(main())
    return 0


# -- operations -------------------------------------------------------------------------


def cmd_db(args: argparse.Namespace) -> int:
    from .migrations_cli import downgrade, upgrade

    settings = _settings()
    _setup_obs("pgw-admin", settings)
    if settings.database_url is None:
        log.error("PGW_DATABASE_URL is not set")
        return 2
    url = settings.database_url.get_secret_value()
    if args.action == "upgrade":
        upgrade(url, args.revision or "head")
    else:
        downgrade(url, args.revision or "-1")
    log.info("database migrated", extra={"action": args.action, "revision": args.revision or "default"})
    return 0


def cmd_partners_create(args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from .auth.partners import PartnerRegistry
    from .runtime import demo_vins
    from .store.sql import SqlPartners, make_engine

    settings = _settings()
    _setup_obs("pgw-admin", settings)
    if settings.database_url is None:
        log.error("PGW_DATABASE_URL is not set: partners are stored in Postgres")
        return 2
    vins = set(args.vin or []) | (set(demo_vins(args.demo_vins)) if args.demo_vins else set())

    async def main() -> dict[str, Any]:
        engine = make_engine(settings.database_url.get_secret_value(), pool_size=1)
        try:
            repo = SqlPartners(async_sessionmaker(engine, expire_on_commit=False))
            p, secret = await PartnerRegistry(repo).register(
                args.name,
                set(args.scopes.split(",")),
                vins,
                rate_per_s=args.rate,
                burst=args.burst,
                webhook_url=args.webhook_url,
            )
        finally:
            await engine.dispose()
        return {
            "partner_id": p.partner_id,
            "client_id": p.client_id,
            "client_secret": secret,
            "webhook_secret": p.webhook_secrets[0] if p.webhook_secrets else None,
            "vins": len(p.vins),
        }

    _out(asyncio.run(main()))
    return 0


def cmd_keys(args: argparse.Namespace) -> int:
    from .auth.keys import KeySet

    path = Path(args.file)
    if args.action == "init":
        if path.exists() and not args.force:
            sys.stderr.write(f"{path} exists; pass --force to replace it\n")
            return 2
        ks = KeySet.new()
        result: dict[str, Any] = {"active": ks.active}
    else:
        ks = KeySet.parse(path.read_text())
        if args.action == "stage":
            result = {"staged": ks.stage()}
        elif args.action == "activate":
            kid = args.kid or next(
                (k["kid"] for k in ks.keys if k["kid"] != ks.active and not k.get("retire_after")), None
            )
            if kid is None:
                sys.stderr.write("no staged key to activate; run `pgw keys stage` first\n")
                return 2
            ks.activate(kid, token_ttl_s=args.token_ttl, leeway_s=60)
            result = {"active": ks.active}
        else:
            result = {"pruned": ks.prune()}
    path.write_text(ks.dumps())
    path.chmod(0o600)
    _out({**result, "kids": [k["kid"] for k in ks.keys], "file": str(path)})
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

    def common(x: argparse.ArgumentParser) -> None:
        x.add_argument("--certs", default=None, help="directory with the mTLS certificates (PGW_CERTS_DIR)")
        x.add_argument("--bus", choices=["memory", "pubsub", "redis"], default=None, help="PGW_BUS")
        x.add_argument("--host", default=None)
        x.add_argument("--vehicles", type=int, default=None, help="number of simulated vehicles (PGW_VEHICLES)")
        x.add_argument("--port", type=int, default=None)

    v = sub.add_parser("vehicle-service", help="run the vehicle service (gRPC)")
    common(v)
    v.add_argument("--time-scale", type=float, default=None)
    v.add_argument("--tls", choices=["mtls", "none"], default=None, help="PGW_GRPC_TLS (none behind Cloud Run)")
    v.add_argument("--backend", choices=["simulated", "smartcar"], default=None, help="PGW_VEHICLE_BACKEND")
    v.set_defaults(fn=cmd_vehicle_service)

    g = sub.add_parser("gateway", help="run the partner gateway")
    common(g)
    g.add_argument("--vehicle-service", default=None, help="host:port (PGW_VEHICLE_SERVICE)")
    g.add_argument("--demo-partner", action="store_true", help="register a demo partner and print its credentials")
    g.add_argument("--webhook-url", default=None)
    g.set_defaults(fn=cmd_gateway)

    sb = sub.add_parser("sandbox", help="run everything in one process with a demo partner")
    sb.add_argument("--port", type=int, default=8080)
    sb.add_argument("--vehicles", type=int, default=10)
    sb.set_defaults(fn=cmd_sandbox)

    db = sub.add_parser("db", help="database migrations (PGW_DATABASE_URL)")
    db.add_argument("action", choices=["upgrade", "downgrade"])
    db.add_argument("--revision", default=None)
    db.set_defaults(fn=cmd_db)

    pa = sub.add_parser("partners", help="manage partners in the configured store")
    pa_sub = pa.add_subparsers(dest="partners_command", required=True)
    pc = pa_sub.add_parser("create", help="register a partner and print its credentials once")
    pc.add_argument("--name", required=True)
    pc.add_argument("--scopes", default="vehicles:read,vehicles:command")
    pc.add_argument("--vin", action="append", help="a VIN the owner shared with this partner (repeatable)")
    pc.add_argument("--demo-vins", type=int, default=0, help="grant the first N simulated vehicles")
    pc.add_argument("--rate", type=float, default=5.0, help="sustained requests per second")
    pc.add_argument("--burst", type=int, default=10)
    pc.add_argument("--webhook-url", default=None)
    pc.set_defaults(fn=cmd_partners_create)

    k = sub.add_parser("keys", help="edit a token signing key set (see pgw/auth/keys.py)")
    k.add_argument("action", choices=["init", "stage", "activate", "prune"])
    k.add_argument("file", help="key set JSON file")
    k.add_argument("--kid", default=None, help="activate: the key to sign with (default: the staged key)")
    k.add_argument("--token-ttl", type=int, default=900, help="activate: token lifetime the old key must outlive")
    k.add_argument("--force", action="store_true")
    k.set_defaults(fn=cmd_keys)

    args = p.parse_args(argv)
    fn: Command = args.fn
    return fn(args)


if __name__ == "__main__":
    sys.exit(main())
