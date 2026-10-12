"""Typed configuration, read from environment variables (prefix `PGW_`).

Every deployable setting lives here; nothing else in the package reads the
environment for configuration. Defaults run the whole system locally with
in-memory stores. A deployment sets the store URLs and a key source:

    PGW_DATABASE_URL=postgresql+asyncpg://pgw:...@/pgw?host=/cloudsql/proj:region:instance
    PGW_REDIS_URL=redis://10.0.0.3:6379/0
    PGW_SIGNING_KEYS_SECRET=projects/proj/secrets/pgw-token-signing-key-dev

`PORT` (set by Cloud Run) is honored as an alias of `PGW_PORT`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PGW_", extra="ignore")

    env: str = "local"
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"

    # network
    host: str = "0.0.0.0"
    port: int = Field(8080, validation_alias=AliasChoices("PGW_PORT", "PORT"))

    # shared state. Unset: in-memory stores (one replica only).
    database_url: SecretStr | None = None
    redis_url: str | None = None
    db_pool_size: int = 10

    # messaging
    bus: Literal["memory", "pubsub", "redis"] = "memory"
    pubsub_project: str = "pgw-local"
    pubsub_suffix: str = ""  # e.g. "-dev", matching the Terraform topic names

    # token signing keys: one of these, or (local only) a key generated at start-up
    signing_keys: SecretStr | None = None
    signing_keys_file: Path | None = None
    signing_keys_secret: str | None = None
    keys_refresh_s: float = 60.0
    token_ttl_s: int = 900

    # gateway behavior
    command_ttl_s: float = 120.0
    webhook_poll_s: float = 1.0
    partner_cache_ttl_s: float = 5.0

    # gateway -> vehicle service
    vehicle_service: str = "localhost:50051"
    vehicle_auth: Literal["mtls", "google-id-token", "insecure"] = "mtls"
    vehicle_timeout_s: float = 1.0
    certs_dir: Path = Path("certs")

    # vehicle service
    grpc_tls: Literal["mtls", "none"] = "mtls"
    vehicle_backend: Literal["simulated", "smartcar"] = "simulated"
    vehicles: int = 50
    time_scale: float = 1.0
    smartcar_api_url: str = "https://api.smartcar.com/v2.0"
    smartcar_auth_url: str = "https://auth.smartcar.com/oauth/token"
    smartcar_client_id: str | None = None
    smartcar_client_secret: SecretStr | None = None
    smartcar_access_token: SecretStr | None = None
    smartcar_refresh_token: SecretStr | None = None

    @model_validator(mode="after")
    def _check(self) -> Settings:
        key_sources = [self.signing_keys, self.signing_keys_file, self.signing_keys_secret]
        if sum(s is not None for s in key_sources) > 1:
            raise ValueError("set only one of PGW_SIGNING_KEYS, PGW_SIGNING_KEYS_FILE, PGW_SIGNING_KEYS_SECRET")
        if self.env != "local" and not any(s is not None for s in key_sources):
            raise ValueError(f"PGW_ENV={self.env}: a signing key source is required (generated keys are local-only)")
        if self.env != "local" and (self.database_url is None) != (self.redis_url is None):
            raise ValueError("set both PGW_DATABASE_URL and PGW_REDIS_URL, or neither")
        if self.vehicle_backend == "smartcar" and not (self.smartcar_access_token or self.smartcar_refresh_token):
            raise ValueError("the smartcar backend needs PGW_SMARTCAR_ACCESS_TOKEN or PGW_SMARTCAR_REFRESH_TOKEN")
        if self.smartcar_refresh_token and not (self.smartcar_client_id and self.smartcar_client_secret):
            raise ValueError("refreshing Smartcar tokens needs PGW_SMARTCAR_CLIENT_ID and PGW_SMARTCAR_CLIENT_SECRET")
        return self

    @property
    def shared_state(self) -> bool:
        return self.database_url is not None and self.redis_url is not None
