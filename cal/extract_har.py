from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from findom.utils.har import (
    entries_from_har,
    find_entries_by_url_substring,
    header_list_to_dict,
    load_har,
    pick_headers_for_smoke,
    request_post_data_text,
    response_text,
    try_parse_json,
)
from findom.utils.redact import redact_headers, redact_payload_shape


OUT_DIR = Path("out")
TOKEN_PATH = OUT_DIR / "token.txt"
SUMMARY_PATH = OUT_DIR / "har_summary.json"
BUNDLE_PATH = OUT_DIR / "token_bundle.json"

LOGIN_URL_SUBSTRING = "connect.cal-online.co.il/col-rest/calconnect/authentication/login"
TX_URL_SUBSTRING = "api.cal-online.co.il/Transactions/api/transactionsDetails/getCardTransactionsDetails"



def _extract_login_token(entries: list[dict[str, Any]]) -> tuple[str | None, dict[str, Any]]:
    login_matches = find_entries_by_url_substring(entries, LOGIN_URL_SUBSTRING)
    login_info: dict[str, Any] = {
        "matches": len(login_matches),
        "token_found": False,
        "innerLoginType": None,
        "hash_present": False,
        "selected_match_number": None,
    }

    last_success_data: dict[str, Any] | None = None
    last_success_match_number: int | None = None
    token: str | None = None
    for idx, entry in enumerate(login_matches, start=1):
        resp_text = response_text(entry)
        data = try_parse_json(resp_text)
        if isinstance(data, dict) and isinstance(data.get("token"), str):
            last_success_data = data
            last_success_match_number = idx

    if last_success_data is not None:
        token = last_success_data["token"]
        login_info["token_found"] = True
        login_info["innerLoginType"] = last_success_data.get("innerLoginType")
        login_info["hash_present"] = "hash" in last_success_data
        login_info["selected_match_number"] = last_success_match_number

    return token, login_info



def _extract_transactions_example(
    entries: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None, dict[str, Any]]:
    tx_matches = find_entries_by_url_substring(entries, TX_URL_SUBSTRING)
    tx_info: dict[str, Any] = {
        "matches": len(tx_matches),
        "selected_match_number": None,
        "selected_entry_index": None,
        "selected_url": None,
    }
    if not tx_matches:
        return None, "Не найден endpoint transactions в HAR.", tx_info

    candidate = tx_matches[-1]
    candidate_entry_index: int | None = None
    for entry_idx, entry in enumerate(entries):
        if entry is candidate:
            candidate_entry_index = entry_idx
            break

    req = candidate.get("request", {})
    url = str(req.get("url", ""))
    method = str(req.get("method", ""))
    headers_list = req.get("headers", [])
    headers = header_list_to_dict(headers_list)
    tx_info["selected_match_number"] = len(tx_matches)
    tx_info["selected_entry_index"] = candidate_entry_index
    tx_info["selected_url"] = url

    body_text = request_post_data_text(candidate)
    body_json = try_parse_json(body_text)

    if body_text and body_json is None:
        return None, "Тело transactions-запроса не JSON, smoke тест невозможен без валидного JSON payload.", tx_info

    if body_json is None:
        return None, "В HAR отсутствует request.postData.text для transactions endpoint.", tx_info

    example = {
        "method": method,
        "url": url,
        "headers": headers,
        "header_keys": list(headers.keys()),
        "request_body": body_json,
    }
    return example, None, tx_info



def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Extract token and transactions request from HAR")
    parser.add_argument("--har", required=True, help="Path to HAR file, e.g. out/cal.har")
    args = parser.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    har = load_har(args.har)
    entries = entries_from_har(har)

    token, login_info = _extract_login_token(entries)
    if token:
        TOKEN_PATH.write_text(token, encoding="utf-8")
    else:
        print(
            "Токен login не найден. Возможные причины: response body не попал в HAR, "
            "ответ был закодирован нестандартно или не было успешного логина."
        )

    tx_example, tx_error, tx_info = _extract_transactions_example(entries)

    summary: dict[str, Any] = {
        "har_file": str(args.har),
        "entries_total": len(entries),
        "login": {
            "endpoint_contains": LOGIN_URL_SUBSTRING,
            **login_info,
        },
        "transactions": {
            "endpoint_contains": TX_URL_SUBSTRING,
            "found": tx_example is not None,
            **tx_info,
        },
    }

    if tx_example is not None:
        summary["transactions"]["example_request"] = {
            "method": tx_example["method"],
            "url": tx_example["url"],
            "header_keys": tx_example["header_keys"],
            "headers": redact_headers(tx_example["headers"]),
            "request_body_structure": redact_payload_shape(tx_example["request_body"]),
        }

        bundle = {
            "transactions_request": {
                "method": tx_example["method"],
                "url": tx_example["url"],
                "headers": pick_headers_for_smoke(tx_example["headers"]),
                "json_body": tx_example["request_body"],
            },
        }
        BUNDLE_PATH.write_text(
            json.dumps(bundle, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    else:
        summary["transactions"]["error"] = tx_error
        print(tx_error)

    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if token:
        print(f"Токен извлечён и сохранён: {TOKEN_PATH}")
    if tx_info["matches"]:
        print(
            "Найдено "
            f"{tx_info['matches']} запросов getCardTransactionsDetails. "
            f"Выбран последний (№{tx_info['selected_match_number']})."
        )
        if tx_info["selected_url"]:
            print(f"Выбранный запрос URL: {tx_info['selected_url']}")
    else:
        print("Найдено 0 запросов getCardTransactionsDetails.")
    print(f"Сводка HAR сохранена: {SUMMARY_PATH}")
    if tx_example is not None:
        print(f"Bundle для smoke теста сохранён: {BUNDLE_PATH}")


if __name__ == "__main__":
    main()
