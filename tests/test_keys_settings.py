"""Signing keys from providers, key-set rotation, typed settings and the admin commands."""

from __future__ import annotations

import json
import time

import jwt
import pytest
from pydantic import ValidationError

from pgw.auth.keys import (
    FileKeyProvider,
    KeyConfigError,
    KeyRing,
    KeySet,
    SecretManagerKeyProvider,
    StaticKeyProvider,
    provider_from_env,
)
from pgw.auth.partners import PartnerRegistry
from pgw.auth.tokens import TokenError, TokenService
from pgw.cli import main
from pgw.runtime import build_gateway, key_provider
from pgw.settings import Settings


async def test_ring_loads_a_key_set_and_publishes_every_key():
    ks = KeySet.new()
    staged = ks.stage()
    ring = await KeyRing.from_provider(StaticKeyProvider(ks.dumps()))
    assert ring.active.kid == ks.active
    assert {k["kid"] for k in ring.jwks()["keys"]} == {ks.active, staged}


async def test_rotation_through_the_key_set_keeps_tokens_valid_across_replicas(tmp_path):
    path = tmp_path / "keyset.json"
    ks = KeySet.new()
    path.write_text(ks.dumps())
    # two replicas reading the same secret
    a = await KeyRing.from_provider(FileKeyProvider(path))
    b = await KeyRing.from_provider(FileKeyProvider(path))
    partner, _ = await PartnerRegistry().register("x", {"vehicles:read"}, set())
    old_token, *_ = TokenService(a).issue(partner)

    staged = ks.stage()
    path.write_text(ks.dumps())
    assert await b.refresh() is True
    assert staged in {k["kid"] for k in b.jwks()["keys"]} and b.active.kid == ks.active  # published, not signing

    ks.activate(staged, token_ttl_s=900, leeway_s=60)
    path.write_text(ks.dumps())
    await a.refresh()
    await b.refresh()
    new_token, *_ = TokenService(a).issue(partner)
    assert jwt.get_unverified_header(new_token)["kid"] == staged
    assert TokenService(b).verify(old_token).partner_id == partner.partner_id  # old key still verifies on b
    assert TokenService(b).verify(new_token).partner_id == partner.partner_id

    assert ks.prune(now=time.time() + 961) != []
    path.write_text(ks.dumps())
    await b.refresh()
    with pytest.raises(TokenError, match="unknown signing key"):
        TokenService(b).verify(old_token)


async def test_a_failed_refresh_keeps_the_current_keys(tmp_path):
    path = tmp_path / "keyset.json"
    path.write_text(KeySet.new().dumps())
    ring = await KeyRing.from_provider(FileKeyProvider(path))
    kid = ring.active.kid
    path.write_text("not json")
    assert await ring.refresh() is False and ring.active.kid == kid
    with pytest.raises(KeyConfigError):
        await KeyRing.from_provider(FileKeyProvider(path))  # but a replica can't start without keys


def test_key_set_validation():
    with pytest.raises(KeyConfigError):
        KeySet.parse('{"active": "k1", "keys": []}')
    ks = KeySet.new()
    with pytest.raises(KeyConfigError):
        ks.activate("k-missing", token_ttl_s=900, leeway_s=60)
    bad = json.loads(ks.dumps())
    bad["keys"][0]["private_pem"] = "-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n"
    with pytest.raises(ValueError):
        KeySet.parse(json.dumps(bad)).signing_keys()


async def test_secret_manager_provider_reads_the_latest_version():
    ks = KeySet.new()

    class Payload:
        data = ks.dumps().encode()

    class Client:
        names: list[str] = []

        async def access_secret_version(self, name):
            self.names.append(name)
            return type("Resp", (), {"payload": Payload()})()

    provider = SecretManagerKeyProvider("projects/p/secrets/pgw-token-signing-key-dev", _client=Client())
    assert (await provider.load()).active == ks.active
    assert Client.names == ["projects/p/secrets/pgw-token-signing-key-dev/versions/latest"]


def test_provider_from_env(tmp_path):
    assert provider_from_env({}) is None
    assert isinstance(provider_from_env({"PGW_SIGNING_KEYS": "{}"}), StaticKeyProvider)
    assert isinstance(provider_from_env({"PGW_SIGNING_KEYS_FILE": str(tmp_path)}), FileKeyProvider)
    assert isinstance(provider_from_env({"PGW_SIGNING_KEYS_SECRET": "projects/p/secrets/s"}), SecretManagerKeyProvider)


def test_settings_from_environment(monkeypatch):
    monkeypatch.setenv("PORT", "9090")
    monkeypatch.setenv("PGW_BUS", "redis")
    monkeypatch.setenv("PGW_SIGNING_KEYS", KeySet.new().dumps())
    s = Settings()
    assert s.port == 9090 and s.bus == "redis" and not s.shared_state
    assert isinstance(key_provider(s), StaticKeyProvider)
    assert "PRIVATE KEY" not in repr(s)  # secrets don't leak into logs


def test_settings_refuse_unsafe_combinations(tmp_path):
    with pytest.raises(ValidationError, match="only one of"):
        Settings(signing_keys="{}", signing_keys_file=tmp_path)
    with pytest.raises(ValidationError, match="both PGW_DATABASE_URL and PGW_REDIS_URL"):
        Settings(env="prod", signing_keys="{}", redis_url="redis://x")
    with pytest.raises(ValidationError, match="smartcar backend needs"):
        Settings(vehicle_backend="smartcar")
    with pytest.raises(ValidationError, match="CLIENT_ID"):
        Settings(vehicle_backend="smartcar", smartcar_refresh_token="r")


def test_keys_command_rotation(tmp_path, capsys):
    f = tmp_path / "keyset.json"
    assert main(["keys", "init", str(f)]) == 0
    first = json.loads(capsys.readouterr().out)["active"]
    assert main(["keys", "init", str(f)]) == 2  # never overwrites keys by accident
    capsys.readouterr()
    assert main(["keys", "stage", str(f)]) == 0
    staged = json.loads(capsys.readouterr().out)["staged"]
    assert main(["keys", "activate", str(f)]) == 0
    assert json.loads(capsys.readouterr().out)["active"] == staged
    ks = KeySet.parse(f.read_text())
    assert {k["kid"] for k in ks.keys} == {first, staged} and oct(f.stat().st_mode & 0o777) == "0o600"
    assert main(["keys", "prune", str(f)]) == 0
    assert json.loads(capsys.readouterr().out)["pruned"] == []  # old key still within its tokens' lifetime


async def test_a_deployed_gateway_refuses_to_generate_its_own_keys():
    with pytest.raises(ValueError, match="signing key source"):
        await build_gateway(Settings(env="prod", vehicle_auth="insecure"))


async def test_build_gateway_from_settings_with_in_memory_stores():
    rt = await build_gateway(Settings(vehicle_auth="insecure", vehicle_service="127.0.0.1:1"))
    try:
        assert rt.gateway.keys.active is not None
        assert rt.gateway.stores is rt.stores
    finally:
        await rt.close()


def test_json_logs_carry_extra_fields_and_the_trace_id(capsys):
    import logging

    from pgw.obs import logging as obs_logging
    from pgw.obs import tracing

    obs_logging.setup("test-service", "INFO", "json")
    try:
        with tracing.tracer("t").start_as_current_span("work"):
            logging.getLogger("pgw.test").info("command applied", extra={"command_id": "cmd_1"})
        line = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    finally:
        logging.getLogger().handlers.clear()
    assert line["message"] == "command applied" and line["command_id"] == "cmd_1"
    assert line["severity"] == "INFO" and line["service"] == "test-service" and len(line["trace_id"]) == 32
