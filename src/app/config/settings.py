from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_VAR_NAME = "APP_ENV"
_VALID_ENVS = {"local", "staging", "prod"}
_DEFAULT_ENV = "local"
_CONFIG_DIR = Path("config")
_DOTENV_PATH = Path(".env")
_DOMAIN_CONFIG = ConfigDict(extra="allow")
_SETTINGS_CONFIG = SettingsConfigDict(
    env_prefix="APP_",
    env_file=".env",
    env_nested_delimiter="__",
    extra="ignore",
)


def _read_env_value_from_dotenv(env_file: Path, key: str) -> str | None:
    if not env_file.exists():
        return None
    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].lstrip()
            if "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() != key:
                continue
            value = value.strip()
            if value and value[0] in ("'", '"') and value[-1:] == value[0]:
                value = value[1:-1]
            return value
    except OSError:
        return None
    return None


def _select_env() -> str:
    env_value = os.getenv(_ENV_VAR_NAME)
    if not env_value:
        env_value = _read_env_value_from_dotenv(_DOTENV_PATH, _ENV_VAR_NAME)
    env = (env_value or _DEFAULT_ENV).strip().lower()
    if env not in _VALID_ENVS:
        valid = ", ".join(sorted(_VALID_ENVS))
        raise ValueError(f"Invalid {_ENV_VAR_NAME}={env!r}. Valid values: {valid}")
    return env


def load_yaml_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a mapping at {path}")
    return data


def _load_yaml_settings(env: str) -> Dict[str, Any]:
    config_dir = _CONFIG_DIR
    base_config = load_yaml_file(config_dir / "base.yaml")
    env_config = load_yaml_file(config_dir / f"{env}.yaml")
    return _merge_configs(base_config, env_config)


def _merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_configs(merged[key], value)
        else:
            merged[key] = value
    return merged


def _yaml_settings_source(*_args: object, **_kwargs: object) -> Dict[str, Any]:
    env_name = _select_env()
    merged = _load_yaml_settings(env_name)
    merged.setdefault("env", env_name)
    return merged


class DomainSettings(BaseModel):
    model_config = _DOMAIN_CONFIG


class DatabaseSettings(DomainSettings):
    host: str = "localhost"
    port: int = Field(default=5432, ge=1, le=65535)
    name: str = "spark_fuse"
    user: str = "spark_fuse"
    pool_size: int = Field(default=5, ge=1)
    connect_timeout_seconds: int = Field(default=10, ge=1)
    password: str | None = Field(default=None, repr=False)


class LoggingSettings(DomainSettings):
    level: str = "INFO"
    json: bool = False

    @field_validator("level")
    @classmethod
    def _normalize_level(cls, value: str) -> str:
        level = str(value).strip().upper()
        allowed = {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"}
        if level not in allowed:
            raise ValueError(f"Unsupported log level: {value!r}")
        return level


class SecuritySettings(DomainSettings):
    allowed_hosts: list[str] = Field(default_factory=list)
    token_ttl_seconds: int = Field(default=3600, ge=60)
    api_key: str | None = Field(default=None, repr=False)


class Settings(BaseSettings):
    env: str = _DEFAULT_ENV
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)

    model_config = _SETTINGS_CONFIG

    @field_validator("env", mode="before")
    @classmethod
    def _normalize_env(cls, value: str) -> str:
        env = str(value).strip().lower() if value is not None else _DEFAULT_ENV
        if env not in _VALID_ENVS:
            valid = ", ".join(sorted(_VALID_ENVS))
            raise ValueError(f"Invalid {_ENV_VAR_NAME}={env!r}. Valid values: {valid}")
        return env

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            env_settings,
            dotenv_settings,
            _yaml_settings_source,
            init_settings,
            file_secret_settings,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings; use this instead of instantiating Settings directly."""
    return Settings()
