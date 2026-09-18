"""Application configuration loaded from config.yaml + environment."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class RepositoryConfig:
    owner: str
    name: str

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.name}"


@dataclass(frozen=True)
class Settings:
    database_url: str
    github_token: str | None
    per_page: int
    timeout: int
    rate_limit_floor: int
    max_pages_per_run: int
    refresh_recent_pages: int
    repositories: list[RepositoryConfig] = field(default_factory=list)


def _resolve_database_url(url: str) -> str:
    """Anchor relative SQLite paths to the project root so cwd does not matter."""
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return url
    raw = url[len(prefix):]
    if raw == ":memory:" or Path(raw).is_absolute():
        return url
    return prefix + str((PROJECT_ROOT / raw).resolve())


def load_settings(config_path: str | Path | None = None) -> Settings:
    if config_path is None:
        config_path = os.getenv("MONITORING_CONFIG") or None
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")

    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}

    db = raw.get("database", {})
    gh = raw.get("github", {})
    col = raw.get("collection", {})

    token_env = gh.get("token_env", "GITHUB_TOKEN")
    token = os.getenv(token_env) or None

    repos = [
        RepositoryConfig(owner=str(r["owner"]), name=str(r["name"]))
        for r in raw.get("repositories", [])
    ]
    if not repos:
        raise ValueError("config.yaml must define at least one repository")

    return Settings(
        database_url=_resolve_database_url(db.get("url", "sqlite:///data/monitoring.db")),
        github_token=token,
        per_page=int(gh.get("per_page", 100)),
        timeout=int(gh.get("timeout", 30)),
        rate_limit_floor=int(gh.get("rate_limit_floor", 2)),
        max_pages_per_run=int(col.get("max_pages_per_run", 10)),
        refresh_recent_pages=int(col.get("refresh_recent_pages", 1)),
        repositories=repos,
    )
