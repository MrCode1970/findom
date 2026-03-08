from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

from tools.connectors._core.errors import InvalidCredentialsError, RateLimitError, TemporaryError
from tools.connectors._core.normalize.dates import parse_date
from tools.connectors._core.retry import retry_call
from tools.connectors._core.types import SyncWindow
from tools.connectors.providers.discount import discover_api
from tools.connectors.providers.discount.fetch import (
    STORAGE_STATE_PATH,
    build_session,
    fetch_by_date,
    load_api_hints,
    ping_keepalive,
    validate_session,
)


LOG = logging.getLogger(__name__)


def _compact_hints(hints: Any) -> dict[str, Any]:
    return {
        "by_date_url_template": hints.by_date_url_template,
        "keepalive_url": hints.keepalive_url,
        "account_number": hints.account_number,
        "required_headers": list(hints.required_headers),
    }


def _run_relogin_discovery() -> Path:
    debug = os.getenv("DISCOUNT_DEBUG", "0") == "1"
    result = discover_api.run_discovery(debug=debug)
    if result != 0:
        raise InvalidCredentialsError(
            "Discount relogin/discovery failed. Check credentials and diagnostics."
        )
    if not STORAGE_STATE_PATH.exists():
        raise InvalidCredentialsError("Discount relogin completed but storage_state.json is missing")
    return STORAGE_STATE_PATH


def _effective_from_date(window: SyncWindow, state: dict[str, Any]) -> Any:
    from_date = window.from_date
    last_sync = state.get("last_sync")
    if last_sync:
        overlap = parse_date(last_sync) - timedelta(days=3)
        if overlap < from_date:
            from_date = overlap
    if from_date > window.to_date:
        from_date = window.to_date
    return from_date


def ensure_session(state: dict[str, Any]) -> tuple[Any, Any, dict[str, Any]]:
    hints = load_api_hints()

    candidate_state = dict(state)
    candidate_state.setdefault("storage_state_path", str(STORAGE_STATE_PATH))

    try:
        session = build_session(candidate_state, hints)
    except TemporaryError:
        LOG.info("Discount storage state missing/invalid, running relogin discovery")
        storage_path = _run_relogin_discovery()
        candidate_state["storage_state_path"] = str(storage_path)
        hints = load_api_hints()
        session = build_session(candidate_state, hints)

    try:
        validate_session(session, hints)
    except InvalidCredentialsError:
        session.close()
        LOG.info("Discount session expired, running relogin discovery")
        storage_path = _run_relogin_discovery()
        candidate_state["storage_state_path"] = str(storage_path)
        hints = load_api_hints()
        session = build_session(candidate_state, hints)
        validate_session(session, hints)

    state_update = {
        "storage_state_path": candidate_state.get("storage_state_path"),
        "api_hints_cache": _compact_hints(hints),
    }
    return session, hints, state_update


def sync(window: SyncWindow, state: dict[str, Any]) -> dict[str, Any]:
    session, hints, state_update = ensure_session(state)
    from_date = _effective_from_date(window, state)

    try:
        # Imitate user activity to reduce idle-session expiry before heavy calls.
        retry_call(
            lambda: ping_keepalive(session, hints),
            exceptions=(TemporaryError, RateLimitError),
        )

        try:
            payload = retry_call(
                lambda: fetch_by_date(
                    session,
                    hints,
                    from_date=from_date,
                    to_date=window.to_date,
                ),
                exceptions=(TemporaryError, RateLimitError),
            )
        except InvalidCredentialsError:
            session.close()
            merged_state = dict(state)
            merged_state.update(state_update)
            relogin_session, relogin_hints, relogin_state = ensure_session(merged_state)
            state_update.update(relogin_state)
            payload = retry_call(
                lambda: fetch_by_date(
                    relogin_session,
                    relogin_hints,
                    from_date=from_date,
                    to_date=window.to_date,
                ),
                exceptions=(TemporaryError, RateLimitError),
            )
            session = relogin_session
            hints = relogin_hints

        state_update["last_sync"] = window.to_date.isoformat()

        return {
            "last_transactions": payload,
            "request_meta": {
                "account_number": hints.account_number,
                "from_date": from_date.isoformat(),
                "to_date": window.to_date.isoformat(),
            },
            "_state_update": state_update,
        }
    finally:
        session.close()
