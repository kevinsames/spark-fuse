"""Configuration entrypoints for application templates."""

from app.config.settings import (  # noqa: F401
    DatabaseSettings,
    DomainSettings,
    LoggingSettings,
    SecuritySettings,
    Settings,
    get_settings,
)

__all__ = [
    "DatabaseSettings",
    "DomainSettings",
    "LoggingSettings",
    "SecuritySettings",
    "Settings",
    "get_settings",
]
