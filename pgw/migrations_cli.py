"""Run the Alembic migrations shipped inside the package (`pgw db upgrade`)."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from alembic import command
from alembic.config import Config

_DIR = Path(__file__).parent / "migrations"


def config(database_url: str) -> Config:
    """Alembic config for the packaged migrations. The URL reaches env.py as `-x url=...`."""
    cfg = Config(str(_DIR / "alembic.ini"), cmd_opts=Namespace(x=[f"url={database_url}"]))
    cfg.set_main_option("script_location", str(_DIR))
    return cfg


def upgrade(database_url: str, revision: str = "head") -> None:
    command.upgrade(config(database_url), revision)


def downgrade(database_url: str, revision: str) -> None:
    command.downgrade(config(database_url), revision)
