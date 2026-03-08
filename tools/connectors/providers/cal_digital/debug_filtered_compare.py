from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.connectors.providers.cal_digital.fetch import build_history_request_body, load_api_hints


NETWORK_LOG = Path(".state/cal_digital/api_discovery/network_log.full.jsonl")


def _safe_header_keys(headers: dict[str, Any]) -> list[str]:
    return sorted(str(key) for key in headers.keys())


def _extract_trans_count(event: dict[str, Any]) -> int | None:
    response = event.get("response", {})
    body_json = response.get("body_json")
    if isinstance(body_json, dict):
        arr = (body_json.get("result") or {}).get("transArr")
        if isinstance(arr, list):
            return len(arr)

    body_file = event.get("artifacts", {}).get("body_file")
    if not body_file:
        return None
    path = Path(str(body_file))
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    arr = (payload.get("result") or {}).get("transArr")
    return len(arr) if isinstance(arr, list) else None


def _load_filtered_responses() -> list[dict[str, Any]]:
    if not NETWORK_LOG.exists():
        raise RuntimeError(f"Network log not found: {NETWORK_LOG}")

    events: list[dict[str, Any]] = []
    for line in NETWORK_LOG.read_text(encoding="utf-8").splitlines():
        if "filteredTransactions/getFilteredTransactions" not in line:
            continue
        event = json.loads(line)
        if event.get("event_type") != "response":
            continue
        events.append(event)
    return events


def _snapshot_from_event(event: dict[str, Any]) -> dict[str, Any]:
    request = event.get("request", {})
    post_data = request.get("post_data_json", {}) if isinstance(request, dict) else {}
    response = event.get("response", {}) if isinstance(event.get("response"), dict) else {}
    return {
        "event_id": event.get("event_id"),
        "method": request.get("method"),
        "url": request.get("url"),
        "header_keys": _safe_header_keys(request.get("headers", {})),
        "body": {
            "bankAccountUniqueID": post_data.get("bankAccountUniqueID"),
            "cards_count": len(post_data.get("cards") or []),
            "fromTransDate": post_data.get("fromTransDate"),
            "toTransDate": post_data.get("toTransDate"),
            "trnType": post_data.get("trnType"),
            "caller": post_data.get("caller"),
            "walletTranInd": post_data.get("walletTranInd"),
        },
        "status": response.get("status"),
        "transArr_count": _extract_trans_count(event),
    }


def _build_current_filtered_snapshot(ui_event: dict[str, Any]) -> dict[str, Any]:
    api_hints = load_api_hints()
    endpoint = api_hints.get("filtered_transactions") or api_hints.get("history")
    if not isinstance(endpoint, dict):
        raise RuntimeError("filtered endpoint is missing in api_hints")

    ui_post = ui_event.get("request", {}).get("post_data_json", {})
    cards = list(ui_post.get("cards") or [])
    card_ids = [str(item.get("cardUniqueID")) for item in cards if isinstance(item, dict) and item.get("cardUniqueID")]
    bank_account = str(ui_post.get("bankAccountUniqueID") or "")
    init_context = {
        "bank_account_unique_id": bank_account,
        "card_unique_ids": card_ids,
    }
    body = build_history_request_body(
        endpoint,
        from_date="2026-02-01",
        to_date="2026-03-06",
        init_context=init_context,
    )
    if not isinstance(body, dict):
        raise RuntimeError("Built body is not a JSON object")

    return {
        "method": endpoint.get("method"),
        "url": endpoint.get("url"),
        "header_keys": _safe_header_keys(endpoint.get("header_templates", {})),
        "body": {
            "bankAccountUniqueID": body.get("bankAccountUniqueID"),
            "cards_count": len(body.get("cards") or []),
            "fromTransDate": body.get("fromTransDate"),
            "toTransDate": body.get("toTransDate"),
            "trnType": body.get("trnType"),
            "caller": body.get("caller"),
            "walletTranInd": body.get("walletTranInd"),
        },
    }


def main() -> int:
    events = _load_filtered_responses()
    if not events:
        raise RuntimeError("No filteredTransactions responses found in network log")

    module_search_events = [
        item
        for item in events
        if (item.get("request", {}).get("post_data_json", {}) or {}).get("caller") == "module_search"
    ]
    dashboard_events = [
        item
        for item in events
        if (item.get("request", {}).get("post_data_json", {}) or {}).get("caller") == "dashboard"
    ]

    ui_reference = module_search_events[-1] if module_search_events else events[-1]
    dashboard_reference = dashboard_events[-1] if dashboard_events else None

    result = {
        "ui_filtered_reference": _snapshot_from_event(ui_reference),
        "ui_dashboard_reference": _snapshot_from_event(dashboard_reference) if dashboard_reference else None,
        "python_built_filtered_request": _build_current_filtered_snapshot(ui_reference),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
