"""Shared HTTP utilities for spark-fuse data source connectors."""

from __future__ import annotations

import logging
import time
from typing import Any, Mapping, Optional

import requests

_LOGGER = logging.getLogger(__name__)


def normalize_jsonable(value: Any) -> Any:
    """Recursively normalize a value into a JSON-serializable form."""
    if isinstance(value, Mapping):
        return {str(k): normalize_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [normalize_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def validate_http_url(value: str) -> bool:
    """Return ``True`` when *value* starts with ``http://`` or ``https://``."""
    return isinstance(value, str) and value.startswith(("http://", "https://"))


def perform_request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    request_kwargs: Optional[Mapping[str, Any]] = None,
) -> Optional[Mapping[str, Any]]:
    """Execute an HTTP request with exponential-backoff retries.

    Returns the decoded JSON response on success or ``None`` when all attempts
    are exhausted.  Callers that need to raise on failure should check the
    return value.
    """
    attempts = max(max_retries, 0) + 1
    kwargs: dict[str, Any] = {"timeout": timeout}
    if request_kwargs:
        kwargs.update(request_kwargs)

    for attempt in range(attempts):
        try:
            response = session.request(method, url, **kwargs)
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError:
                    _LOGGER.error("Failed to decode JSON response from %s", url)
                    return None
            _LOGGER.warning(
                "HTTP %s from %s (attempt %s/%s)",
                response.status_code,
                url,
                attempt + 1,
                attempts,
            )
        except requests.RequestException as exc:
            _LOGGER.warning(
                "Request to %s failed on attempt %s/%s: %s",
                url,
                attempt + 1,
                attempts,
                exc,
            )
        if attempt < attempts - 1:
            delay = backoff_factor * (2**attempt)
            if delay > 0:
                time.sleep(delay)

    _LOGGER.error("Exhausted retries fetching %s", url)
    return None
