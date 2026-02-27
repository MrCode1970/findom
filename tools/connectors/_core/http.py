from __future__ import annotations

import logging
from typing import Any

import requests

from tools.connectors._core.errors import (
    InvalidCredentialsError,
    RateLimitError,
    TemporaryError,
)


LOG = logging.getLogger(__name__)
DEFAULT_TIMEOUT = (5, 20)


def _raise_for_status(response: requests.Response) -> None:
    status = response.status_code

    if status in (401, 403):
        raise InvalidCredentialsError(f"Auth failed with status={status}")
    if status == 429:
        raise RateLimitError("Rate limit reached")
    if 500 <= status < 600:
        raise TemporaryError(f"Provider temporary error status={status}")
    if status >= 400:
        raise TemporaryError(f"Unexpected provider status={status}")


def _parse_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        raise TemporaryError("Provider returned non-JSON response") from exc


def get_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
) -> Any:
    LOG.debug("GET %s params=%s", url, params)
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
    except requests.Timeout as exc:
        raise TemporaryError("Request timed out") from exc
    except requests.RequestException as exc:
        raise TemporaryError("Request failed") from exc

    _raise_for_status(response)
    return _parse_json(response)


def post_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
) -> Any:
    LOG.debug("POST %s params=%s", url, params)
    try:
        response = requests.post(
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=timeout,
        )
    except requests.Timeout as exc:
        raise TemporaryError("Request timed out") from exc
    except requests.RequestException as exc:
        raise TemporaryError("Request failed") from exc

    _raise_for_status(response)
    return _parse_json(response)
