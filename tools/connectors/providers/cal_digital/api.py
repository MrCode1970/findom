from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import requests

from tools.connectors._core.errors import InvalidCredentialsError, RateLimitError, TemporaryError
from tools.connectors._core.normalize.dates import parse_date
from tools.connectors._core.retry import retry_call
from tools.connectors._core.types import SyncWindow
from tools.connectors.providers.cal_digital import discover_api
from tools.connectors.providers.cal_digital.fetch import (
    build_session,
    fetch_cards,
    fetch_transactions,
    load_api_hints,
)


LOG = logging.getLogger(__name__)


def _normalize_key(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _extract_card_id(card: dict[str, Any]) -> str | None:
    for key in (
        "cardId",
        "card_id",
        "id",
        "accountId",
        "cardNumber",
        "pan",
        "last4",
    ):
        value = card.get(key)
        if value is not None and str(value).strip():
            return str(value)

    for key, value in card.items():
        normalized = _normalize_key(key)
        if "card" in normalized and "id" in normalized and str(value).strip():
            return str(value)

    return None


def _collect_card_objects(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        prioritized = (
            "cards",
            "cardList",
            "items",
            "data",
            "results",
            "content",
        )
        for key in prioritized:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        for value in payload.values():
            found = _collect_card_objects(value)
            if found:
                return found

    return []


def _compact_hints(api_hints: dict[str, Any]) -> dict[str, Any]:
    return {
        "base_url": api_hints.get("base_url"),
        "report_generated_at": api_hints.get("report_generated_at"),
        "init": {
            "method": api_hints.get("init", {}).get("method"),
            "url_template": api_hints.get("init", {}).get("url_template"),
            "required_headers": api_hints.get("init", {}).get("required_headers", []),
        },
        "history": {
            "method": api_hints.get("history", {}).get("method"),
            "url_template": api_hints.get("history", {}).get("url_template"),
            "required_headers": api_hints.get("history", {}).get("required_headers", []),
            "pagination_hints": api_hints.get("history", {}).get("pagination_hints", []),
            "date_fields": api_hints.get("history", {}).get("date_fields", []),
        },
        "dashboard": {
            "method": api_hints.get("dashboard", {}).get("method"),
            "url_template": api_hints.get("dashboard", {}).get("url_template"),
            "required_headers": api_hints.get("dashboard", {}).get("required_headers", []),
        },
    }


def _run_relogin_discovery(base_url: str) -> Path:
    debug = os.getenv("CAL_DEBUG", "0") == "1"
    result = discover_api.run_discovery(base_url=base_url, debug=debug)
    if result != 0:
        raise InvalidCredentialsError(
            "CAL relogin/discovery failed. Check credentials and diagnostics report."
        )

    if not discover_api.STORAGE_STATE_PATH.exists():
        raise InvalidCredentialsError("CAL relogin finished but storage_state.json is missing")

    return discover_api.STORAGE_STATE_PATH


def _validate_session(session: requests.Session, api_hints: dict[str, Any]) -> None:
    def operation() -> dict | list:
        return fetch_cards(session, api_hints)

    retry_call(operation, exceptions=(TemporaryError, RateLimitError))


def ensure_session(state: dict[str, Any]) -> tuple[requests.Session, dict[str, Any], dict[str, Any]]:
    api_hints = load_api_hints()

    candidate_state = dict(state)
    candidate_state.setdefault("storage_state_path", api_hints.get("storage_state_path"))

    try:
        session = build_session(candidate_state, api_hints)
    except TemporaryError:
        LOG.info("Existing storage state is not usable, running relogin discovery")
        storage_path = _run_relogin_discovery(api_hints.get("base_url", discover_api.DEFAULT_BASE_URL))
        candidate_state["storage_state_path"] = str(storage_path)
        api_hints = load_api_hints()
        session = build_session(candidate_state, api_hints)

    try:
        _validate_session(session, api_hints)
    except InvalidCredentialsError:
        session.close()
        LOG.info("Session expired, running relogin discovery")
        storage_path = _run_relogin_discovery(api_hints.get("base_url", discover_api.DEFAULT_BASE_URL))
        candidate_state["storage_state_path"] = str(storage_path)
        api_hints = load_api_hints()
        session = build_session(candidate_state, api_hints)

        try:
            _validate_session(session, api_hints)
        except InvalidCredentialsError as exc:
            session.close()
            raise InvalidCredentialsError(
                "CAL credentials are invalid or additional auth is required"
            ) from exc

    new_state = {
        "storage_state_path": candidate_state.get("storage_state_path"),
        "api_hints_cache": _compact_hints(api_hints),
    }
    return session, api_hints, new_state


def _effective_from_date(window: SyncWindow, state: dict[str, Any]) -> str:
    from_date = window.from_date
    last_sync = state.get("last_sync")
    if last_sync:
        overlap_from = parse_date(last_sync) - timedelta(days=7)
        if overlap_from < from_date:
            from_date = overlap_from

    if from_date > window.to_date:
        from_date = window.to_date

    return from_date.isoformat()


def _fetch_cards_with_relogin(
    session: requests.Session,
    api_hints: dict[str, Any],
    state: dict[str, Any],
    state_update: dict[str, Any],
) -> tuple[requests.Session, dict[str, Any], dict | list]:
    try:
        cards_payload = retry_call(
            lambda: fetch_cards(session, api_hints),
            exceptions=(TemporaryError, RateLimitError),
        )
        return session, api_hints, cards_payload
    except InvalidCredentialsError:
        session.close()
        merged_state = dict(state)
        merged_state.update(state_update)
        relogin_session, relogin_hints, relogin_state = ensure_session(merged_state)
        state_update.update(relogin_state)
        cards_payload = retry_call(
            lambda: fetch_cards(relogin_session, relogin_hints),
            exceptions=(TemporaryError, RateLimitError),
        )
        return relogin_session, relogin_hints, cards_payload


def sync(window: SyncWindow, state: dict[str, Any]) -> dict[str, Any]:
    session, api_hints, state_update = ensure_session(state)
    from_date = _effective_from_date(window, state)
    to_date = window.to_date.isoformat()

    try:
        session, api_hints, cards_payload = _fetch_cards_with_relogin(
            session,
            api_hints,
            state,
            state_update,
        )

        cards = _collect_card_objects(cards_payload)
        card_ids: list[str] = []
        for card in cards:
            card_id = _extract_card_id(card)
            if card_id and card_id not in card_ids:
                card_ids.append(card_id)

        txns_by_card: dict[str, list[dict[str, Any]]] = {}
        if not card_ids:
            card_ids = [""]
        for card_id in card_ids:
            try:
                transactions = retry_call(
                    lambda cid=card_id: fetch_transactions(
                        session,
                        api_hints,
                        card_id=cid or None,
                        from_date=from_date,
                        to_date=to_date,
                        init_payload=cards_payload,
                    ),
                    exceptions=(TemporaryError, RateLimitError),
                )
            except InvalidCredentialsError:
                session.close()
                merged_state = dict(state)
                merged_state.update(state_update)
                session, api_hints, relogin_state = ensure_session(merged_state)
                state_update.update(relogin_state)
                try:
                    transactions = retry_call(
                        lambda cid=card_id: fetch_transactions(
                            session,
                            api_hints,
                            card_id=cid or None,
                            from_date=from_date,
                            to_date=to_date,
                            init_payload=cards_payload,
                        ),
                        exceptions=(TemporaryError, RateLimitError),
                    )
                except InvalidCredentialsError as exc:
                    raise InvalidCredentialsError(
                        "CAL API returned 401/403 even after relogin"
                    ) from exc
            txns_by_card[card_id or "unknown"] = transactions

        state_update["last_sync"] = window.to_date.isoformat()

        return {
            "cards": cards_payload,
            "txns_by_card": txns_by_card,
            "_state_update": state_update,
        }
    finally:
        session.close()
