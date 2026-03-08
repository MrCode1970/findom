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
    fetch_clearance_requests,
    fetch_filtered_transactions,
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
        "clearance": {
            "method": api_hints.get("clearance", {}).get("method"),
            "url_template": api_hints.get("clearance", {}).get("url_template"),
            "required_headers": api_hints.get("clearance", {}).get("required_headers", []),
            "pagination_hints": api_hints.get("clearance", {}).get("pagination_hints", []),
            "date_fields": api_hints.get("clearance", {}).get("date_fields", []),
        },
        "filtered_transactions": {
            "method": api_hints.get("filtered_transactions", {}).get("method"),
            "url_template": api_hints.get("filtered_transactions", {}).get("url_template"),
            "required_headers": api_hints.get("filtered_transactions", {}).get("required_headers", []),
            "pagination_hints": api_hints.get("filtered_transactions", {}).get("pagination_hints", []),
            "date_fields": api_hints.get("filtered_transactions", {}).get("date_fields", []),
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


def _fetch_filtered_with_relogin(
    session: requests.Session,
    api_hints: dict[str, Any],
    state: dict[str, Any],
    state_update: dict[str, Any],
    *,
    from_date: str,
    to_date: str,
    init_payload: Any,
) -> tuple[requests.Session, dict[str, Any], list[dict[str, Any]]]:
    try:
        filtered = retry_call(
            lambda: fetch_filtered_transactions(
                session,
                api_hints,
                card_id=None,
                from_date=from_date,
                to_date=to_date,
                init_payload=init_payload,
            ),
            exceptions=(TemporaryError, RateLimitError),
        )
        return session, api_hints, filtered
    except InvalidCredentialsError:
        session.close()
        merged_state = dict(state)
        merged_state.update(state_update)
        relogin_session, relogin_hints, relogin_state = ensure_session(merged_state)
        state_update.update(relogin_state)
        filtered = retry_call(
            lambda: fetch_filtered_transactions(
                relogin_session,
                relogin_hints,
                card_id=None,
                from_date=from_date,
                to_date=to_date,
                init_payload=init_payload,
            ),
            exceptions=(TemporaryError, RateLimitError),
        )
        return relogin_session, relogin_hints, filtered


def _fetch_clearance_with_relogin(
    session: requests.Session,
    api_hints: dict[str, Any],
    state: dict[str, Any],
    state_update: dict[str, Any],
    *,
    from_date: str,
    to_date: str,
    init_payload: Any,
) -> tuple[requests.Session, dict[str, Any], list[dict[str, Any]]]:
    try:
        clearance = retry_call(
            lambda: fetch_clearance_requests(
                session,
                api_hints,
                card_id=None,
                from_date=from_date,
                to_date=to_date,
                init_payload=init_payload,
            ),
            exceptions=(TemporaryError, RateLimitError),
        )
        return session, api_hints, clearance
    except InvalidCredentialsError:
        session.close()
        merged_state = dict(state)
        merged_state.update(state_update)
        relogin_session, relogin_hints, relogin_state = ensure_session(merged_state)
        state_update.update(relogin_state)
        clearance = retry_call(
            lambda: fetch_clearance_requests(
                relogin_session,
                relogin_hints,
                card_id=None,
                from_date=from_date,
                to_date=to_date,
                init_payload=init_payload,
            ),
            exceptions=(TemporaryError, RateLimitError),
        )
        return relogin_session, relogin_hints, clearance


def _first_text_value(txn: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = txn.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _transaction_dedupe_key(card_id: str, txn: dict[str, Any]) -> str:
    transaction_id = _first_text_value(
        txn,
        ("trnIntId", "transactionId", "txnId", "operationId", "id"),
    )
    txn_card_id = _first_text_value(
        txn,
        ("cardUniqueID", "cardUniqueId", "cardId", "card_id"),
    )
    effective_card = txn_card_id or card_id or "unknown"
    if transaction_id:
        return f"id:{effective_card}:{transaction_id}"

    occurred_at = _first_text_value(
        txn,
        ("trnPurchaseDate", "debCrdDate", "transactionDate", "date", "eventDate", "createdAt"),
    )
    amount = _first_text_value(
        txn,
        ("trnAmt", "amount", "debitAmount", "creditAmount", "transactionAmount"),
    )
    merchant = _first_text_value(
        txn,
        ("merchantName", "merchant", "businessName", "storeName", "description"),
    )
    return f"fallback:{effective_card}:{occurred_at}:{amount}:{merchant}"


def _merge_history_transactions(
    *,
    card_id: str,
    clearance_items: list[dict[str, Any]],
    filtered_items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    merged: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for source_endpoint, items in (
        ("clearance", clearance_items),
        ("filtered", filtered_items),
    ):
        for item in items:
            if not isinstance(item, dict):
                continue
            decorated = dict(item)
            decorated["source_endpoint"] = source_endpoint
            dedupe_key = _transaction_dedupe_key(card_id, decorated)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            merged.append(decorated)

    return merged, {
        "clearance_raw": len(clearance_items),
        "filtered_raw": len(filtered_items),
        "merged_unique": len(merged),
    }


def _extract_txn_card_id(txn: dict[str, Any]) -> str:
    return _first_text_value(txn, ("cardUniqueID", "cardUniqueId", "cardId", "card_id")) or "unknown"


def _group_transactions_by_card(transactions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for txn in transactions:
        if not isinstance(txn, dict):
            continue
        card_key = _extract_txn_card_id(txn)
        grouped.setdefault(card_key, []).append(txn)
    return grouped


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

        session, api_hints, filtered_all = _fetch_filtered_with_relogin(
            session,
            api_hints,
            state,
            state_update,
            from_date=from_date,
            to_date=to_date,
            init_payload=cards_payload,
        )
        filtered_by_card = _group_transactions_by_card(filtered_all)

        session, api_hints, clearance_all = _fetch_clearance_with_relogin(
            session,
            api_hints,
            state,
            state_update,
            from_date=from_date,
            to_date=to_date,
            init_payload=cards_payload,
        )
        clearance_by_card = _group_transactions_by_card(clearance_all)

        txns_by_card: dict[str, list[dict[str, Any]]] = {}
        history_diagnostics: dict[str, dict[str, int]] = {}
        if not card_ids:
            card_ids = [""]
        loop_card_ids = list(card_ids)
        for extra_card in filtered_by_card.keys():
            if extra_card and extra_card not in loop_card_ids:
                loop_card_ids.append(extra_card)
        for extra_card in clearance_by_card.keys():
            if extra_card and extra_card not in loop_card_ids:
                loop_card_ids.append(extra_card)

        for card_id in loop_card_ids:
            card_key = card_id or "unknown"
            clearance_transactions = clearance_by_card.get(card_key, [])
            filtered_transactions = filtered_by_card.get(card_key, [])
            merged_transactions, card_stats = _merge_history_transactions(
                card_id=card_id,
                clearance_items=clearance_transactions,
                filtered_items=filtered_transactions,
            )
            txns_by_card[card_key] = merged_transactions
            history_diagnostics[card_key] = card_stats
            LOG.info(
                "CAL card %s history merge: clearance=%s filtered=%s merged=%s",
                card_key,
                card_stats["clearance_raw"],
                card_stats["filtered_raw"],
                card_stats["merged_unique"],
            )

        state_update["last_sync"] = window.to_date.isoformat()

        total_clearance_raw = sum(item["clearance_raw"] for item in history_diagnostics.values())
        total_filtered_raw = sum(item["filtered_raw"] for item in history_diagnostics.values())
        total_merged_unique = sum(item["merged_unique"] for item in history_diagnostics.values())

        return {
            "cards": cards_payload,
            "txns_by_card": txns_by_card,
            "history_diagnostics": {
                "per_card": history_diagnostics,
                "totals": {
                    "clearance_raw": total_clearance_raw,
                    "filtered_raw": total_filtered_raw,
                    "merged_unique": total_merged_unique,
                },
            },
            "_state_update": state_update,
        }
    finally:
        session.close()
