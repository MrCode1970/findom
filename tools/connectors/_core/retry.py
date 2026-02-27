from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

from tools.connectors._core.errors import RateLimitError, TemporaryError


LOG = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass(slots=True, frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 8.0
    backoff_factor: float = 2.0


def retry_call(
    operation: Callable[[], T],
    *,
    exceptions: tuple[type[Exception], ...] = (TemporaryError, RateLimitError),
    config: RetryConfig = RetryConfig(),
    sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    if config.max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    attempt = 0
    while True:
        attempt += 1
        try:
            return operation()
        except exceptions as exc:
            if attempt >= config.max_attempts:
                raise

            delay = min(
                config.base_delay * (config.backoff_factor ** (attempt - 1)),
                config.max_delay,
            )
            LOG.warning(
                "Retryable error on attempt %s/%s: %s. Sleeping %.2fs",
                attempt,
                config.max_attempts,
                exc,
                delay,
            )
            sleep_fn(delay)
