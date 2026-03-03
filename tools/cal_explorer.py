from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import signal
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse
import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if TYPE_CHECKING:
    from playwright.sync_api import BrowserContext, Page, Request, Response

if load_dotenv is not None:
    load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_URL = "https://digital-web.cal-online.co.il/"
DEFAULT_LOG_DIR = Path("/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery")
DEFAULT_CAPTURE_CONTAINS = [
    "/transactions/api/filteredtransactions/getfilteredtransactions",
    "/transactions/api/approvals/getclearancerequests",
    "/transactions/api/lasttransactionsfordashboard",
    "/account/init",
    "/transactiondetails",
]
DEFAULT_SEED_REQUEST_PATH = Path("/home/vitaliy/mr1970code/findom/.state/cal_digital/api_discovery/seed_filtered_request.json")
DEFAULT_WARMUP_ROUTES = (
    "/transactions-search",
    "/all-actions",
)

NETWORK_LOG_FILE = "network_log.full.jsonl"
RESPONSES_DIR = "responses"
CSV_FILE = "transactions_log.csv"
MD_FILE = "transactions_log.md"

INLINE_BODY_MAX_BYTES = 200 * 1024

SENSITIVE_HEADER_MARKERS = (
    "authorization",
    "cookie",
    "set-cookie",
    "token",
    "secret",
    "password",
    "csrf",
)
SENSITIVE_FIELD_MARKERS = (
    "authorization",
    "token",
    "tokennumber",
    "tokenind",
    "wallettokenind",
    "cookie",
    "password",
    "secret",
    "csrf",
)


@dataclass
class ExplorerState:
    log_dir: Path
    response_dir: Path
    network_log_path: Path
    csv_path: Path
    md_path: Path
    capture_contains: list[str]
    event_id: int = 0
    request_events: int = 0
    response_events: int = 0
    captured_bodies: int = 0
    extracted_transactions: int = 0
    any_request_events: int = 0
    any_response_events: int = 0
    login_submitted: bool = False
    replay_attempts: int = 0
    replay_successes: int = 0
    replay_failures: int = 0
    history_days: int = 90
    tx_rows_current: int = 0
    tx_update_events: int = 0
    active_pull_url: str | None = None
    active_pull_headers: dict[str, str] | None = None
    active_pull_body: dict[str, Any] | None = None
    active_pull_template_captured: bool = False
    known_bank_account_id: str | None = None
    known_card_ids: list[str] | None = None
    active_pull_attempts: int = 0
    active_pull_successes: int = 0
    active_pull_failures: int = 0
    active_pull_last_monotonic: float = 0.0
    active_pull_approvals_url: str | None = None
    active_pull_approvals_headers: dict[str, str] | None = None
    active_pull_approvals_body: dict[str, Any] | None = None
    active_pull_approvals_template_captured: bool = False
    active_pull_approvals_attempts: int = 0
    active_pull_approvals_successes: int = 0
    active_pull_approvals_failures: int = 0
    active_pull_approvals_last_monotonic: float = 0.0


@dataclass
class LiveSheetWriter:
    enabled: bool
    spreadsheet_id: str | None
    min_interval_sec: float = 2.0
    last_push_monotonic: float = 0.0

    def push_snapshot(self, rows_by_key: dict[str, dict[str, str]]) -> None:
        if not self.enabled or not self.spreadsheet_id:
            return
        now_mono = time.monotonic()
        if (now_mono - self.last_push_monotonic) < self.min_interval_sec:
            return

        write_log_snapshot = getattr(import_module("scripts.sync_log_to_sheet"), "write_log_snapshot")
        synced_at = _utc_now_iso()
        movements: list[dict[str, str]] = []
        for row in rows_by_key.values():
            account_id = row.get("cardUniqueId", "").strip()
            if account_id:
                account_id = f"cal:{account_id}"
            movements.append(
                {
                    "date": row.get("trnPurchaseDate") or row.get("debCrdDate") or "",
                    "amount": row.get("amountForDisplay", ""),
                    "currency": "ILS",
                    "merchant": row.get("merchantName", ""),
                    "comment": "",
                    "account_id": account_id,
                    "source": "cal_digital_explorer",
                    "external_id": row.get("external_id", ""),
                }
            )
        write_log_snapshot(
            spreadsheet_id=self.spreadsheet_id,
            movements=movements,
            synced_at_iso=synced_at,
        )
        self.last_push_monotonic = now_mono


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_capture_contains(raw: str | None) -> list[str]:
    if not raw:
        return [item.lower() for item in DEFAULT_CAPTURE_CONTAINS]
    values = [item.strip() for item in raw.split(",")]
    filtered = [item.lower() for item in values if item]
    return filtered or list(DEFAULT_CAPTURE_CONTAINS)


def _is_sensitive_name(name: str) -> bool:
    text = name.lower()
    return any(marker in text for marker in SENSITIVE_FIELD_MARKERS)


def _redact_headers(headers: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in headers.items():
        key_lower = key.lower()
        if any(marker in key_lower for marker in SENSITIVE_HEADER_MARKERS):
            out[key] = "<redacted>"
        else:
            out[key] = value
    return out


def _sanitize_json(value: Any, parent_key: str = "") -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, sub_value in value.items():
            if _is_sensitive_name(key):
                out[key] = "<redacted>"
            else:
                out[key] = _sanitize_json(sub_value, key)
        return out
    if isinstance(value, list):
        return [_sanitize_json(item, parent_key) for item in value]
    if isinstance(value, str) and _is_sensitive_name(parent_key):
        return "<redacted>"
    return value


def _request_post_data_json(request: "Request") -> Any | None:
    try:
        payload = request.post_data
    except Exception:
        return None
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except Exception:
        return None
    return _sanitize_json(parsed)


def _request_post_data_json_raw(request: "Request") -> Any | None:
    try:
        payload = request.post_data
    except Exception:
        return None
    if not payload:
        return None
    try:
        return json.loads(payload)
    except Exception:
        return None


def _url_matches_filters(url: str, filters: list[str]) -> bool:
    url_l = url.lower()
    return any(token in url_l for token in filters)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _response_artifact_path(state: ExplorerState, event_id: int, extension: str) -> Path:
    return state.response_dir / f"event_{event_id}.{extension}"


def _try_parse_json(text: str) -> tuple[Any | None, str | None]:
    try:
        return json.loads(text), None
    except Exception as exc:
        return None, str(exc)


def _is_api_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host.endswith("api.cal-online.co.il")


def _replay_request_text(
    request: "Request",
    *,
    override_json: dict[str, Any] | None = None,
) -> tuple[str | None, str | None, int | None]:
    method = (request.method or "GET").upper()
    if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
        return None, "unsupported_method", None
    if not _is_api_url(request.url):
        return None, "not_api_url", None

    headers = dict(request.headers)
    headers = _prepare_request_headers(headers)

    data = request.post_data
    if override_json is not None:
        data = json.dumps(override_json, ensure_ascii=False)
    kwargs: dict[str, Any] = {"headers": headers, "timeout": 30}
    if data is not None:
        kwargs["data"] = data.encode("utf-8")

    try:
        resp = requests.request(method, request.url, **kwargs)
    except Exception as exc:
        return None, f"replay_request_failed: {exc}", None
    if resp.status_code >= 400:
        return None, f"replay_status={resp.status_code}", resp.status_code
    return resp.text, None, resp.status_code


def _prepare_request_headers(headers: dict[str, str]) -> dict[str, str]:
    prepared = dict(headers)
    drop_headers = {
        "content-length",
        "host",
        "connection",
        "accept-encoding",
        "origin",
        "referer",
    }
    for key in list(prepared):
        low = key.lower()
        if low in drop_headers or low.startswith("sec-ch-"):
            prepared.pop(key, None)
    return prepared


def _maybe_build_wide_filtered_body(request: "Request", history_days: int) -> dict[str, Any] | None:
    if "filteredTransactions/getFilteredTransactions" not in request.url:
        return None
    src = _request_post_data_json(request)
    if not isinstance(src, dict):
        return None
    return _build_wide_filtered_body_from_source(src, history_days)


def _build_wide_filtered_body_from_source(src: dict[str, Any], history_days: int) -> dict[str, Any]:
    body = dict(src)
    # Снимаем узкие фильтры, чтобы получить полный набор движений.
    body["trnType"] = 0
    body["merchantHebName"] = ""
    body["merchantHebCity"] = ""
    body["fromTrnAmt"] = 0
    body["toTrnAmt"] = 0
    body["transactionsOrigin"] = 0
    body["transCardPresentInd"] = 0
    body["walletTranInd"] = 0
    # По живому запросу из DevTools более стабильный режим для истории.
    body["caller"] = "module_search"
    now_utc = datetime.now(timezone.utc)
    from_utc = now_utc - timedelta(days=max(1, history_days))
    body["fromTransDate"] = from_utc.isoformat().replace("+00:00", "Z")
    body["toTransDate"] = now_utc.isoformat().replace("+00:00", "Z")
    return body


def _build_fallback_filtered_body(
    bank_account_id: str,
    card_ids: list[str],
    history_days: int,
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    from_utc = now_utc - timedelta(days=max(1, history_days))
    return {
        "bankAccountUniqueID": bank_account_id,
        "cards": [{"cardUniqueID": card_id} for card_id in card_ids if card_id],
        "fromTransDate": from_utc.isoformat().replace("+00:00", "Z"),
        "toTransDate": now_utc.isoformat().replace("+00:00", "Z"),
        "merchantHebName": "",
        "merchantHebCity": "",
        "trnType": 0,
        "fromTrnAmt": 0,
        "toTrnAmt": 0,
        "transactionsOrigin": 0,
        "transCardPresentInd": 0,
        "walletTranInd": 0,
        "caller": "module_search",
    }


def _build_wide_approvals_body_from_source(src: dict[str, Any], history_days: int) -> dict[str, Any]:
    body = dict(src)
    filter_params = body.get("filterParams")
    if not isinstance(filter_params, dict):
        filter_params = {}
    else:
        filter_params = dict(filter_params)
    now_utc = datetime.now(timezone.utc)
    from_utc = now_utc - timedelta(days=max(1, history_days))
    filter_params["fromTransDate"] = from_utc.isoformat().replace("+00:00", "Z")
    # approvals endpoint обычно использует конец дня
    filter_params["toTransDate"] = now_utc.isoformat().replace("+00:00", "Z")
    for key in (
        "merchantID",
        "merchantHebName",
        "merchantHebCity",
        "trnType",
        "fromTrnAmt",
        "toTrnAmt",
        "transactionsOrigin",
        "transCardPresentInd",
        "walletTranInd",
    ):
        filter_params[key] = None
    body["filterParams"] = filter_params
    return body


def _build_fallback_approvals_body(card_ids: list[str], history_days: int) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    from_utc = now_utc - timedelta(days=max(1, history_days))
    return {
        "cardUniqueIDArray": [card_id for card_id in card_ids if card_id],
        "filterParams": {
            "fromTransDate": from_utc.isoformat().replace("+00:00", "Z"),
            "toTransDate": now_utc.isoformat().replace("+00:00", "Z"),
            "merchantID": None,
            "merchantHebName": None,
            "merchantHebCity": None,
            "trnType": None,
            "fromTrnAmt": None,
            "toTrnAmt": None,
            "transactionsOrigin": None,
            "transCardPresentInd": None,
            "walletTranInd": None,
        },
    }


def _extract_init_context(payload: Any) -> tuple[str | None, list[str] | None]:
    if not isinstance(payload, dict):
        return None, None
    result = payload.get("result")
    if not isinstance(result, dict):
        return None, None
    bank_account_id: str | None = None
    cards: list[str] = []
    bank_accounts = result.get("bankAccounts")
    if isinstance(bank_accounts, list) and bank_accounts:
        first = bank_accounts[0]
        if isinstance(first, dict):
            for key in ("bankAccountUniqueId", "bankAccountUniqueID"):
                value = first.get(key)
                if value:
                    bank_account_id = str(value).strip()
                    break
    cards_arr = result.get("cards")
    if isinstance(cards_arr, list):
        for card in cards_arr:
            if isinstance(card, dict):
                value = card.get("cardUniqueId") or card.get("cardUniqueID")
                if value:
                    cards.append(str(value).strip())
    return bank_account_id, [c for c in cards if c]


def _active_pull_filtered_transactions(
    state: ExplorerState,
    sheet_writer: LiveSheetWriter | None,
    min_interval_sec: float,
) -> None:
    if not state.active_pull_url or not state.active_pull_headers or not state.active_pull_body:
        return
    now = time.monotonic()
    if (now - state.active_pull_last_monotonic) < min_interval_sec:
        return
    state.active_pull_last_monotonic = now
    state.active_pull_attempts += 1

    try:
        wide_body = _build_wide_filtered_body_from_source(state.active_pull_body, state.history_days)
        headers = _prepare_request_headers(state.active_pull_headers)
        response = requests.post(
            state.active_pull_url,
            headers=headers,
            data=json.dumps(wide_body, ensure_ascii=False).encode("utf-8"),
            timeout=30,
        )
        if response.status_code >= 400:
            state.active_pull_failures += 1
            return
        payload = response.json()
        tx_count = _trans_count(payload)
        if tx_count > 0:
            state.active_pull_successes += 1
            _update_transactions_logs(
                state=state,
                sheet_writer=sheet_writer,
                source_url=state.active_pull_url,
                captured_at=_utc_now_iso(),
                payload_json=payload,
            )
    except Exception:
        state.active_pull_failures += 1


def _active_pull_approvals_transactions(
    state: ExplorerState,
    sheet_writer: LiveSheetWriter | None,
    min_interval_sec: float,
) -> None:
    if (
        not state.active_pull_approvals_url
        or not state.active_pull_approvals_headers
        or not state.active_pull_approvals_body
    ):
        return
    now = time.monotonic()
    if (now - state.active_pull_approvals_last_monotonic) < min_interval_sec:
        return
    state.active_pull_approvals_last_monotonic = now
    state.active_pull_approvals_attempts += 1

    try:
        wide_body = _build_wide_approvals_body_from_source(
            state.active_pull_approvals_body,
            state.history_days,
        )
        headers = _prepare_request_headers(state.active_pull_approvals_headers)
        response = requests.post(
            state.active_pull_approvals_url,
            headers=headers,
            data=json.dumps(wide_body, ensure_ascii=False).encode("utf-8"),
            timeout=30,
        )
        if response.status_code >= 400:
            state.active_pull_approvals_failures += 1
            return
        payload = response.json()
        tx_count = _trans_count(payload)
        if tx_count > 0:
            state.active_pull_approvals_successes += 1
            _update_transactions_logs(
                state=state,
                sheet_writer=sheet_writer,
                source_url=state.active_pull_approvals_url,
                captured_at=_utc_now_iso(),
                payload_json=payload,
            )
    except Exception:
        state.active_pull_approvals_failures += 1


def _active_pull_transactions(
    state: ExplorerState,
    sheet_writer: LiveSheetWriter | None,
    min_interval_sec: float,
) -> None:
    _active_pull_approvals_transactions(state, sheet_writer, min_interval_sec)
    _active_pull_filtered_transactions(state, sheet_writer, min_interval_sec)


def _maybe_prepare_fallback_active_pull(state: ExplorerState) -> None:
    if state.active_pull_template_captured:
        # approvals может быть не пойман отдельно, поэтому не выходим здесь
        pass
    if (
        state.active_pull_url
        and state.active_pull_body
        and state.active_pull_headers
        and state.active_pull_approvals_url
        and state.active_pull_approvals_body
        and state.active_pull_approvals_headers
    ):
        return
    if not state.known_bank_account_id or not state.known_card_ids:
        return
    if not state.active_pull_headers:
        return
    if not state.active_pull_url:
        state.active_pull_url = "https://api.cal-online.co.il/Transactions/api/filteredTransactions/getFilteredTransactions"
    if not state.active_pull_body:
        state.active_pull_body = _build_fallback_filtered_body(
            bank_account_id=state.known_bank_account_id,
            card_ids=state.known_card_ids,
            history_days=state.history_days,
        )
    if not state.active_pull_approvals_url:
        state.active_pull_approvals_url = (
            "https://api.cal-online.co.il/Transactions/api/approvals/getClearanceRequests"
        )
    if not state.active_pull_approvals_body:
        state.active_pull_approvals_body = _build_fallback_approvals_body(
            card_ids=state.known_card_ids,
            history_days=state.history_days,
        )
    if not state.active_pull_approvals_headers:
        state.active_pull_approvals_headers = dict(state.active_pull_headers)


def _run_warmup_routes(page: "Page", base_url: str) -> None:
    for route in DEFAULT_WARMUP_ROUTES:
        try:
            page.goto(base_url.rstrip("/") + route, wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(1200)
        except Exception:
            continue


def _pump_events(page: "Page", milliseconds: int = 250) -> None:
    try:
        page.wait_for_timeout(milliseconds)
    except Exception:
        # During shutdown the driver can be disconnected; ignore noisy errors here.
        pass


def _safe_unlink(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _fresh_reset_artifacts(state: ExplorerState, clear_network_log: bool = True) -> None:
    _safe_unlink(state.csv_path)
    _safe_unlink(state.md_path)
    if clear_network_log:
        _safe_unlink(state.network_log_path)
    try:
        if state.response_dir.exists():
            shutil.rmtree(state.response_dir, ignore_errors=True)
    except Exception:
        pass
    state.response_dir.mkdir(parents=True, exist_ok=True)


def _load_seed_request(path: Path) -> tuple[str, dict[str, str], dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    url = str(payload.get("url", "")).strip()
    headers = payload.get("headers")
    body = payload.get("body")
    if not url or not isinstance(headers, dict):
        return None
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            return None
    if not isinstance(body, dict):
        return None
    return url, {str(k): str(v) for k, v in headers.items()}, body


def _extract_query_params(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    return {k: (v[-1] if v else "") for k, v in query.items()}


def _request_payload(request: "Request") -> dict[str, Any]:
    headers = request.headers
    return {
        "method": request.method,
        "url": request.url,
        "resource_type": request.resource_type,
        "headers": _redact_headers(dict(headers)),
        "query_params": _extract_query_params(request.url),
        "post_data_json": _request_post_data_json(request),
    }


def _load_existing_transactions(csv_path: Path) -> dict[str, dict[str, str]]:
    if not csv_path.exists():
        return {}
    rows: dict[str, dict[str, str]] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = row.get("external_id", "").strip()
            if key:
                rows[key] = row
    return rows


CSV_COLUMNS = [
    "external_id",
    "trnIntId",
    "cardUniqueId",
    "merchantName",
    "amountForDisplay",
    "currencyForDisplay",
    "trnPurchaseDate",
    "debCrdDate",
    "trnType",
    "transactionTypeCode",
    "refundInd",
    "isImmediate",
    "raw_source_url",
    "captured_at",
]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parse_date_for_sort(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    fixed = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(fixed)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _write_transactions_csv(csv_path: Path, rows_by_key: dict[str, dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(
        rows_by_key.values(),
        key=lambda row: _parse_date_for_sort(row.get("trnPurchaseDate", "")),
        reverse=True,
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in ordered:
            writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def _write_transactions_md(md_path: Path, rows_by_key: dict[str, dict[str, str]]) -> None:
    ordered = sorted(
        rows_by_key.values(),
        key=lambda row: _parse_date_for_sort(row.get("trnPurchaseDate", "")),
        reverse=True,
    )
    latest = ordered[:50]
    all_dates = [_parse_date_for_sort(row.get("trnPurchaseDate", "")) for row in ordered]
    valid_dates = [item for item in all_dates if item > datetime.min.replace(tzinfo=timezone.utc)]

    min_date = valid_dates[-1].date().isoformat() if valid_dates else "-"
    max_date = valid_dates[0].date().isoformat() if valid_dates else "-"
    refund_count = sum(1 for row in ordered if _truthy(row.get("refundInd", "")))

    lines: list[str] = []
    lines.append("# Transactions Log")
    lines.append("")
    lines.append(f"Последнее обновление: {_utc_now_iso()}")
    lines.append("")
    lines.append("## Сводка")
    lines.append(f"- Всего уникальных: {len(ordered)}")
    lines.append(f"- Диапазон дат: {min_date} .. {max_date}")
    lines.append(f"- refundInd=true: {refund_count}")
    lines.append("")
    lines.append("## Последние 50")
    lines.append("")
    lines.append(
        "| external_id | trnPurchaseDate | cardUniqueId | merchantName | amountForDisplay | currencyForDisplay | trnType | refundInd | raw_source_url |"
    )
    lines.append("|---|---|---|---|---:|---|---|---|---|")

    for row in latest:
        lines.append(
            "| {external_id} | {trnPurchaseDate} | {cardUniqueId} | {merchantName} | {amountForDisplay} | {currencyForDisplay} | {trnType} | {refundInd} | {raw_source_url} |".format(
                external_id=row.get("external_id", ""),
                trnPurchaseDate=row.get("trnPurchaseDate", ""),
                cardUniqueId=row.get("cardUniqueId", ""),
                merchantName=(row.get("merchantName", "") or "").replace("|", "\\|"),
                amountForDisplay=row.get("amountForDisplay", ""),
                currencyForDisplay=row.get("currencyForDisplay", ""),
                trnType=(row.get("trnType", "") or "").replace("|", "\\|"),
                refundInd=row.get("refundInd", ""),
                raw_source_url=row.get("raw_source_url", ""),
            )
        )

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _extract_transactions_from_json(payload: Any) -> list[dict[str, Any]]:
    def looks_like_txn(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        txn_keys = {
            "trnIntId",
            "cardUniqueId",
            "merchantName",
            "amountForDisplay",
            "trnPurchaseDate",
            "debCrdDate",
            "transactionTypeCode",
        }
        present = sum(1 for key in txn_keys if key in item)
        return present >= 3

    found: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            if node and all(isinstance(x, dict) for x in node):
                found.extend([x for x in node if looks_like_txn(x)])
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)

    walk(payload)
    def extract_approvals(node: Any) -> list[dict[str, Any]]:
        if not isinstance(node, dict):
            return []
        result = node.get("result")
        if not isinstance(result, dict):
            return []
        arr = result.get("clearanceRequests")
        if not isinstance(arr, list):
            return []
        out: list[dict[str, Any]] = []
        for item in arr:
            if not isinstance(item, dict):
                continue
            approval_id = (
                item.get("clearanceRequestID")
                or item.get("clearanceRequestId")
                or item.get("requestID")
                or item.get("requestId")
            )
            card_id = item.get("cardUniqueId") or item.get("cardUniqueID")
            merchant = item.get("merchantName") or item.get("merchantHebName") or item.get("merchant")
            amount = item.get("amountForDisplay") or item.get("trnAmt") or item.get("amount")
            date_value = item.get("trnPurchaseDate") or item.get("requestDate") or item.get("debCrdDate")
            if not any([approval_id, merchant, amount, date_value]):
                continue
            out.append(
                {
                    "trnIntId": f"approval:{approval_id}" if approval_id else "",
                    "cardUniqueId": str(card_id or ""),
                    "merchantName": str(merchant or ""),
                    "amountForDisplay": amount,
                    "currencyForDisplay": item.get("currencyForDisplay") or item.get("trnCurrencySymbol") or "₪",
                    "trnPurchaseDate": date_value,
                    "debCrdDate": item.get("debCrdDate") or "",
                    "trnType": item.get("trnType") or "clearance",
                    "transactionTypeCode": item.get("transactionTypeCode") or "clearance",
                    "refundInd": bool(item.get("refundInd", False)),
                    "isImmediate": bool(item.get("isImmediate", False)),
                    "_source": "approvals",
                }
            )
        return out

    dedup: dict[str, dict[str, Any]] = {}
    # approvals разбираем первыми, чтобы при пересечениях filteredTransactions
    # мог корректно дообогатить запись полями/датой без ломки хронологии.
    combined = extract_approvals(payload)
    combined.extend(found)
    for txn in combined:
        key = _external_id_from_txn(txn)
        if key:
            dedup[key] = txn
    return list(dedup.values())


def _trans_count(payload: Any) -> int:
    return len(_extract_transactions_from_json(payload))


def _external_id_from_txn(txn: dict[str, Any]) -> str | None:
    trn_int_id = str(txn.get("trnIntId", "")).strip()
    card_unique_id = str(txn.get("cardUniqueId", "")).strip()
    if trn_int_id:
        if card_unique_id:
            return f"{card_unique_id}:{trn_int_id}"
        return trn_int_id
    stable = {
        "card": card_unique_id,
        "merchant": str(txn.get("merchantName", "")).strip(),
        "amount": str(txn.get("amountForDisplay", "")).strip(),
        "date": str(txn.get("trnPurchaseDate", "")).strip() or str(txn.get("debCrdDate", "")).strip(),
        "type": str(txn.get("trnType", "")).strip(),
    }
    if not (stable["merchant"] or stable["amount"] or stable["date"]):
        return None
    digest = hashlib.sha256(json.dumps(stable, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:20]
    if card_unique_id:
        return f"{card_unique_id}:hash:{digest}"
    return f"hash:{digest}"


def _update_transactions_logs(
    state: ExplorerState,
    sheet_writer: LiveSheetWriter | None,
    source_url: str,
    captured_at: str,
    payload_json: Any,
) -> None:
    transactions = _extract_transactions_from_json(payload_json)
    if not transactions:
        return

    rows_by_key = _load_existing_transactions(state.csv_path)
    updated = False

    written_rows = 0
    for txn in transactions:
        external_id = _external_id_from_txn(txn)
        if not external_id:
            continue
        rows_by_key[external_id] = {
            "external_id": external_id,
            "trnIntId": str(txn.get("trnIntId", "")),
            "cardUniqueId": str(txn.get("cardUniqueId", "")),
            "merchantName": str(txn.get("merchantName", "")),
            "amountForDisplay": str(txn.get("amountForDisplay", "")),
            "currencyForDisplay": str(txn.get("currencyForDisplay", "")),
            "trnPurchaseDate": str(txn.get("trnPurchaseDate", "")),
            "debCrdDate": str(txn.get("debCrdDate", "")),
            "trnType": str(txn.get("trnType", "")),
            "transactionTypeCode": str(txn.get("transactionTypeCode", "")),
            "refundInd": str(txn.get("refundInd", "")),
            "isImmediate": str(txn.get("isImmediate", "")),
            "raw_source_url": source_url,
            "captured_at": captured_at,
        }
        updated = True
        written_rows += 1

    if not updated:
        return

    _write_transactions_csv(state.csv_path, rows_by_key)
    _write_transactions_md(state.md_path, rows_by_key)
    state.extracted_transactions += written_rows
    state.tx_rows_current = len(rows_by_key)
    state.tx_update_events += 1
    if sheet_writer is not None:
        try:
            sheet_writer.push_snapshot(rows_by_key)
            if sheet_writer.enabled and sheet_writer.spreadsheet_id:
                print(f"[sheet] Journal updated: {len(rows_by_key)} rows")
        except Exception as exc:
            print(f"[warn] live sheet update failed: {exc}", file=sys.stderr)


def _attach_context_listeners(
    context: "BrowserContext",
    state: ExplorerState,
    sheet_writer: LiveSheetWriter | None,
) -> None:
    def on_request(request: "Request") -> None:
        try:
            state.any_request_events += 1
            if request.resource_type not in {"xhr", "fetch"}:
                return
            if _is_api_url(request.url):
                req_headers = dict(request.headers)
                if req_headers.get("authorization") and req_headers.get("x-site-id"):
                    state.active_pull_headers = req_headers
            if "filteredTransactions/getFilteredTransactions" in request.url and _is_api_url(request.url):
                raw_body = _request_post_data_json_raw(request)
                if isinstance(raw_body, dict):
                    state.active_pull_url = request.url
                    state.active_pull_headers = dict(request.headers)
                    state.active_pull_body = raw_body
                    if not state.active_pull_template_captured:
                        print("[pull] template captured from filteredTransactions request")
                    state.active_pull_template_captured = True
            if "approvals/getClearanceRequests" in request.url and _is_api_url(request.url):
                raw_body = _request_post_data_json_raw(request)
                if isinstance(raw_body, dict):
                    state.active_pull_approvals_url = request.url
                    state.active_pull_approvals_headers = dict(request.headers)
                    state.active_pull_approvals_body = raw_body
                    if not state.active_pull_approvals_template_captured:
                        print("[pull] template captured from approvals request")
                    state.active_pull_approvals_template_captured = True
            state.event_id += 1
            state.request_events += 1
            event = {
                "event_id": state.event_id,
                "event_type": "request",
                "timestamp": _utc_now_iso(),
                "request": _request_payload(request),
                "response": None,
                "artifacts": {},
            }
            _append_jsonl(state.network_log_path, event)
        except Exception as exc:
            print(f"[warn] request logging failed: {exc}", file=sys.stderr)

    def on_response(response: "Response") -> None:
        try:
            request = response.request
            state.any_response_events += 1
            if request.resource_type not in {"xhr", "fetch"}:
                return

            state.event_id += 1
            state.response_events += 1
            captured_at = _utc_now_iso()
            request_data = _request_payload(request)

            event: dict[str, Any] = {
                "event_id": state.event_id,
                "event_type": "response",
                "timestamp": captured_at,
                "request": request_data,
                "response": {
                    "status": response.status,
                    "headers": _redact_headers(dict(response.headers)),
                    "content_type": response.headers.get("content-type", ""),
                },
                "artifacts": {},
            }

            url = request.url
            body_text: str | None = None
            body_size = 0
            body_sha: str | None = None
            body_read_error: str | None = None

            try:
                body_bytes = response.body()
                body_text = body_bytes.decode("utf-8", errors="replace")
                body_size = len(body_bytes)
                body_sha = _sha256_text(body_text)
            except Exception as exc_body:
                body_read_error = str(exc_body)
                try:
                    text_fallback = response.text()
                    body_text = text_fallback
                    body_size = len(text_fallback.encode("utf-8", errors="ignore"))
                    body_sha = _sha256_text(text_fallback)
                except Exception as exc_text:
                    if body_read_error:
                        body_read_error = f"{body_read_error}; text_fallback={exc_text}"
                    else:
                        body_read_error = str(exc_text)

            parsed_json: Any | None = None
            json_error: str | None = None
            content_type = response.headers.get("content-type", "").lower()
            if body_text is not None:
                if "json" in content_type:
                    try:
                        parsed_json = response.json()
                    except Exception:
                        parsed_json, json_error = _try_parse_json(body_text)
                else:
                    parsed_json, json_error = _try_parse_json(body_text)

            artifacts: dict[str, Any] = {
                "body_size_bytes": body_size,
            }
            if body_sha is not None:
                artifacts["body_sha256"] = body_sha
            if body_read_error:
                artifacts["body_read_error"] = body_read_error

            should_capture_full = _url_matches_filters(url, state.capture_contains)
            if parsed_json is not None:
                extracted = _extract_transactions_from_json(parsed_json)
                if extracted:
                    should_capture_full = True

            if should_capture_full:
                state.captured_bodies += 1

            if parsed_json is not None:
                sanitized_json = _sanitize_json(parsed_json)
                json_text = json.dumps(sanitized_json, ensure_ascii=False)
                json_size = len(json_text.encode("utf-8"))
                if should_capture_full:
                    if json_size <= INLINE_BODY_MAX_BYTES:
                        event["response"]["body_json"] = sanitized_json
                    else:
                        body_path = _response_artifact_path(state, state.event_id, "json")
                        body_path.write_text(json_text, encoding="utf-8")
                        artifacts["body_file"] = str(body_path)
                _update_transactions_logs(state, sheet_writer, url, captured_at, parsed_json)
            else:
                if should_capture_full and body_text is not None:
                    body_path = _response_artifact_path(state, state.event_id, "txt")
                    body_path.write_text(body_text, encoding="utf-8")
                    artifacts["body_file"] = str(body_path)
                    if json_error:
                        artifacts["json_parse_error"] = json_error
                elif should_capture_full and body_text is None:
                    artifacts["body_unavailable"] = True

            if parsed_json is None and _is_api_url(url):
                state.replay_attempts += 1
                artifacts["replay_attempted"] = True
                replay_text, replay_error, replay_status = _replay_request_text(request)
                if replay_status is not None:
                    artifacts["replay_status"] = replay_status
                if replay_error:
                    state.replay_failures += 1
                    artifacts["replay_error"] = replay_error
                elif replay_text is not None:
                    replay_json, replay_json_error = _try_parse_json(replay_text)
                    if replay_json is not None:
                        state.replay_successes += 1
                        replay_sanitized = _sanitize_json(replay_json)
                        replay_serialized = json.dumps(replay_sanitized, ensure_ascii=False)
                        replay_size = len(replay_serialized.encode("utf-8"))
                        if should_capture_full:
                            if replay_size <= INLINE_BODY_MAX_BYTES:
                                event["response"]["body_json"] = replay_sanitized
                            else:
                                body_path = _response_artifact_path(state, state.event_id, "json")
                                body_path.write_text(replay_serialized, encoding="utf-8")
                                artifacts["body_file"] = str(body_path)
                        artifacts["replay_used"] = True
                        _update_transactions_logs(state, sheet_writer, url, captured_at, replay_json)
                    else:
                        state.replay_failures += 1
                        artifacts["replay_json_error"] = replay_json_error

            # Контекст account/init часто доступен даже когда filteredTransactions не пойман.
            if _is_api_url(url) and "Authentication/api/account/init" in url:
                src_payload = parsed_json
                if src_payload is None and artifacts.get("replay_used"):
                    # replay_json уже обработан выше, но не сохранен в переменную; мягко пропускаем.
                    src_payload = None
                if src_payload is None and "body_json" in event.get("response", {}):
                    src_payload = event["response"]["body_json"]
                bank_id, card_ids = _extract_init_context(src_payload)
                if bank_id:
                    state.known_bank_account_id = bank_id
                if card_ids:
                    state.known_card_ids = card_ids
                _maybe_prepare_fallback_active_pull(state)

            # Если filteredTransactions отдает мало данных, пробуем "широкий" вариант запроса.
            if (
                parsed_json is not None
                and "filteredTransactions/getFilteredTransactions" in url
                and _trans_count(parsed_json) <= 20
            ):
                wide_body = _maybe_build_wide_filtered_body(request, state.history_days)
                if wide_body is not None:
                    state.replay_attempts += 1
                    artifacts["wide_replay_attempted"] = True
                    wide_text, wide_error, wide_status = _replay_request_text(
                        request,
                        override_json=wide_body,
                    )
                    if wide_status is not None:
                        artifacts["wide_replay_status"] = wide_status
                    if wide_error:
                        state.replay_failures += 1
                        artifacts["wide_replay_error"] = wide_error
                    elif wide_text is not None:
                        wide_json, wide_json_error = _try_parse_json(wide_text)
                        if wide_json is not None:
                            before = _trans_count(parsed_json)
                            after = _trans_count(wide_json)
                            artifacts["wide_replay_trans_count"] = after
                            if after > before:
                                state.replay_successes += 1
                                artifacts["wide_replay_used"] = True
                                wide_sanitized = _sanitize_json(wide_json)
                                wide_serialized = json.dumps(wide_sanitized, ensure_ascii=False)
                                wide_size = len(wide_serialized.encode("utf-8"))
                                if should_capture_full:
                                    if wide_size <= INLINE_BODY_MAX_BYTES:
                                        event["response"]["body_json"] = wide_sanitized
                                    else:
                                        body_path = _response_artifact_path(state, state.event_id, "json")
                                        body_path.write_text(wide_serialized, encoding="utf-8")
                                        artifacts["body_file"] = str(body_path)
                                _update_transactions_logs(state, sheet_writer, url, captured_at, wide_json)
                        else:
                            state.replay_failures += 1
                            artifacts["wide_replay_json_error"] = wide_json_error

            event["artifacts"] = artifacts
            _append_jsonl(state.network_log_path, event)
        except Exception as exc:
            print(f"[warn] response logging failed: {exc}", file=sys.stderr)

    context.on("request", on_request)
    context.on("response", on_response)


def _bootstrap_context_page(context: "BrowserContext", start_url: str) -> "Page":
    pages = context.pages
    if pages:
        page = pages[0]
    else:
        page = context.new_page()
    if page.url in {"", "about:blank"}:
        page.goto(start_url, wait_until="domcontentloaded")
    return page


def _switch_via_js(target: Any, needle: str) -> bool:
    script = """
    (needle) => {
      const has = (text) => (text || '').toLowerCase().includes(needle.toLowerCase());
      const nodes = document.querySelectorAll('button,a,[role="tab"],[role="button"],span,div');
      for (const node of nodes) {
        const text = node.innerText || node.textContent || '';
        if (has(text)) {
          node.click();
          return true;
        }
      }
      return false;
    }
    """
    try:
        return bool(target.evaluate(script, needle))
    except Exception:
        return False


def _fill_via_js_fallback(target: Any, value: str, is_password: bool) -> bool:
    script = """
    ({value, isPassword}) => {
      const has = (text, list) => {
        const low = (text || '').toLowerCase();
        return list.some((item) => low.includes(item));
      };
      const userWords = ["user", "username", "login", "mail", "id", "שם משתמש"];
      const passWords = ["pass", "password", "pwd", "סיס", "קוד"];
      const inputs = Array.from(document.querySelectorAll('input'));
      const candidates = inputs.filter((el) => {
        const type = (el.getAttribute('type') || '').toLowerCase();
        if (isPassword) {
          return type === "password" || has(el.name, passWords) || has(el.id, passWords) || has(el.placeholder, passWords) || has(el.getAttribute("aria-label"), passWords);
        }
        return type !== "password";
      });
      if (!candidates.length) return false;
      const best = candidates[0];
      best.focus();
      best.value = value;
      best.dispatchEvent(new Event('input', { bubbles: true }));
      best.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    }
    """
    try:
        return bool(target.evaluate(script, {"value": value, "isPassword": is_password}))
    except Exception:
        return False


def _switch_to_username_login(page: "Page") -> bool:
    switch_text = "כניסה עם שם משתמש"
    switch_short = "שם משתמש"
    alt_labels = ("כניסה עם סיסמה", "שם משתמש וסיסמה", "password", "username")
    for target in [page, *page.frames]:
        for locator in (
            target.get_by_role("tab", name=switch_text, exact=False),
            target.get_by_role("button", name=switch_text, exact=False),
            target.get_by_role("link", name=switch_text, exact=False),
            target.get_by_text(switch_text, exact=False),
            target.get_by_text(switch_short, exact=False),
            ):
            try:
                if locator.count() > 0:
                    locator.first.click(timeout=1200)
                    return True
            except Exception:
                continue
        for text in alt_labels:
            try:
                locator = target.get_by_text(text, exact=False)
                if locator.count() > 0:
                    locator.first.click(timeout=1200)
                    return True
            except Exception:
                continue
        if _switch_via_js(target, switch_short):
            return True
    return False


def _fill_first(target: Any, selectors: tuple[str, ...], value: str) -> bool:
    for selector in selectors:
        locator = target.locator(selector)
        try:
            if locator.count() <= 0:
                continue
            field = locator.first
            field.click(timeout=1200)
            field.fill(value, timeout=2000)
            return True
        except Exception:
            continue
    return False


def _prefill_credentials(page: "Page", username: str, password: str) -> tuple[bool, bool]:
    username_selectors = (
        "input[autocomplete='username']",
        "input[name='username']",
        "input[name='userName']",
        "input[name='identifier']",
        "input[id*='user']",
        "input[id*='User']",
        "input[placeholder*='שם משתמש']",
        "input[type='text']",
    )
    password_selectors = (
        "input[autocomplete='current-password']",
        "input[name='password']",
        "input[name='pass']",
        "input[id*='pass']",
        "input[type='password']",
    )
    user_ok = False
    pass_ok = False
    for target in [page, *page.frames]:
        if not user_ok:
            user_ok = _fill_first(target, username_selectors, username)
            if not user_ok:
                user_ok = _fill_via_js_fallback(target, username, is_password=False)
        if not pass_ok:
            pass_ok = _fill_first(target, password_selectors, password)
            if not pass_ok:
                pass_ok = _fill_via_js_fallback(target, password, is_password=True)
        if user_ok and pass_ok:
            break
    return user_ok, pass_ok


def _submit_login(page: "Page") -> bool:
    for target in [page, *page.frames]:
        try:
            password_fields = target.locator("input[type='password']")
            if password_fields.count() > 0:
                password_fields.first.press("Enter", timeout=1500)
                return True
        except Exception:
            pass
        for locator in (
            target.get_by_role("button", name=re.compile("כניסה|התחבר|login|sign in", re.I)),
            target.locator("button[type='submit']"),
            target.locator("input[type='submit']"),
        ):
            try:
                if locator.count() > 0:
                    locator.first.click(timeout=1500)
                    return True
            except Exception:
                continue
    return False


def _maybe_auto_login(page: "Page", state: ExplorerState | None = None) -> bool:
    username = (os.getenv("CAL_USERNAME") or "").strip()
    password = (os.getenv("CAL_PASSWORD") or "").strip()
    if not username or not password:
        return False
    current_url = (page.url or "").lower()
    try:
        needs_login = any(token in current_url for token in ("/login", "/calconnect", "login"))
    except Exception:
        needs_login = False
    if not needs_login:
        try:
            needs_login = page.locator("input[type='password']").count() > 0
        except Exception:
            needs_login = False
    if not needs_login:
        return False

    for _ in range(6):
        switched = _switch_to_username_login(page)
        if switched:
            print("[info] Переключил форму на логин/пароль")
        user_ok, pass_ok = _prefill_credentials(page, username, password)
        if user_ok and pass_ok:
            if _submit_login(page):
                print("[info] Автологин отправлен (Enter/submit)")
                page.wait_for_timeout(3000)
                if state is not None:
                    state.login_submitted = True
                return True
        page.wait_for_timeout(800)
    print("[warn] Автозаполнение не удалось, продолжай вручную")
    return False


def _print_startup_instructions(state: ExplorerState) -> None:
    print("CAL Explorer запущен.")
    print("1) Войди на сайт")
    print("2) Открой историю операций")
    print("3) Скроль вниз / меняй фильтры")
    print("4) Смотри обновления в transactions_log.md")
    print("5) Ctrl+C для выхода")
    print(f"Network log: {state.network_log_path}")
    print(f"Transactions CSV: {state.csv_path}")
    print(f"Transactions MD: {state.md_path}")


def _print_live_status(state: ExplorerState) -> None:
    print(
        "[live] any req={any_req} resp={any_resp} | xhr/fetch req={req} resp={resp} captured={cap} tx_rows={tx} tx_updates={txu} | replay a/s/f={ra}/{rs}/{rf} | active_pull tpl={tpl} appr_tpl={atpl} ctx={ctx} a/s/f={apa}/{aps}/{apf} appr_a/s/f={aapa}/{aaps}/{aapf}".format(
            any_req=state.any_request_events,
            any_resp=state.any_response_events,
            req=state.request_events,
            resp=state.response_events,
            cap=state.captured_bodies,
            tx=state.tx_rows_current,
            txu=state.tx_update_events,
            ra=state.replay_attempts,
            rs=state.replay_successes,
            rf=state.replay_failures,
            tpl=("yes" if state.active_pull_template_captured else "no"),
            atpl=("yes" if state.active_pull_approvals_template_captured else "no"),
            ctx=("yes" if state.known_bank_account_id and state.known_card_ids else "no"),
            apa=state.active_pull_attempts,
            aps=state.active_pull_successes,
            apf=state.active_pull_failures,
            aapa=state.active_pull_approvals_attempts,
            aaps=state.active_pull_approvals_successes,
            aapf=state.active_pull_approvals_failures,
        )
    )


def run_explorer(args: argparse.Namespace) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Playwright не установлен. Установи пакет и браузер: "
            "'pip install playwright && playwright install chromium'"
        ) from exc

    log_dir = Path(args.log_dir).expanduser().resolve()
    profile_dir = Path(args.profile_dir).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    state = ExplorerState(
        log_dir=log_dir,
        response_dir=log_dir / RESPONSES_DIR,
        network_log_path=log_dir / NETWORK_LOG_FILE,
        csv_path=log_dir / CSV_FILE,
        md_path=log_dir / MD_FILE,
        capture_contains=_parse_capture_contains(args.capture_contains),
        history_days=max(1, int(args.history_days)),
    )
    state.response_dir.mkdir(parents=True, exist_ok=True)
    if args.fresh_run:
        _fresh_reset_artifacts(state, clear_network_log=True)
        print("[run] fresh mode: reset csv/md/network/responses")
    live_sheet_writer: LiveSheetWriter | None = None
    if args.live_sheet:
        spreadsheet_id = (os.getenv("GOOGLE_SHEETS_ID") or "").strip()
        credentials_path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
        if not spreadsheet_id:
            print("[warn] GOOGLE_SHEETS_ID не задан, live-запись в Google Sheets отключена")
        elif not credentials_path:
            print("[warn] GOOGLE_APPLICATION_CREDENTIALS не задан, live-запись в Google Sheets отключена")
        else:
            live_sheet_writer = LiveSheetWriter(
                enabled=True,
                spreadsheet_id=spreadsheet_id,
                min_interval_sec=max(0.0, float(args.sheet_min_interval)),
            )
            print(f"[sheet] live update enabled -> {spreadsheet_id}")
            if args.fresh_run and args.clear_sheet_on_start:
                try:
                    write_log_snapshot = getattr(import_module("scripts.sync_log_to_sheet"), "write_log_snapshot")
                    write_log_snapshot(spreadsheet_id=spreadsheet_id, movements=[], synced_at_iso=_utc_now_iso())
                    print("[sheet] Journal cleared for fresh run")
                except Exception as exc:
                    print(f"[warn] cannot clear Journal on start: {exc}", file=sys.stderr)

    seed_path = Path(args.seed_request_file).expanduser().resolve()
    seed = _load_seed_request(seed_path)
    if seed is not None:
        url, headers, body = seed
        state.active_pull_url = url
        state.active_pull_headers = headers
        state.active_pull_body = body
        state.active_pull_template_captured = True
        print(f"[pull] seed loaded from {seed_path}")

    should_stop = False

    def handle_sigint(_sig: int, _frame: Any) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, handle_sigint)

    with sync_playwright() as pw:
        launch_args: list[str] = []
        no_viewport = False
        if args.headful:
            launch_args.append("--start-maximized")
            no_viewport = True
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=not args.headful,
            ignore_https_errors=True,
            no_viewport=no_viewport,
            args=launch_args,
        )
        try:
            page = _bootstrap_context_page(context, args.url)
            _attach_context_listeners(context, state, live_sheet_writer)
            if args.auto_login:
                _maybe_auto_login(page, state)
                def on_new_page(new_page: "Page") -> None:
                    try:
                        _maybe_auto_login(new_page, state)
                    except Exception:
                        pass
                context.on("page", on_new_page)
            last_status_print = time.time()
            last_auto_login_retry = time.time()
            last_warmup_retry = 0.0
            _print_startup_instructions(state)

            if args.keep_open:
                while not should_stop:
                    if args.auto_login and not state.login_submitted and (time.time() - last_auto_login_retry >= 5):
                        try:
                            _maybe_auto_login(page, state)
                        except Exception:
                            pass
                        last_auto_login_retry = time.time()
                    if time.time() - last_status_print >= 5:
                        _print_live_status(state)
                        last_status_print = time.time()
                    if args.active_pull:
                        _maybe_prepare_fallback_active_pull(state)
                        _active_pull_transactions(
                            state=state,
                            sheet_writer=live_sheet_writer,
                            min_interval_sec=max(1.0, float(args.pull_interval)),
                        )
                    if (
                        args.auto_warmup
                        and state.login_submitted
                        and not state.active_pull_template_captured
                        and (time.time() - last_warmup_retry >= max(5.0, float(args.warmup_interval)))
                    ):
                        _run_warmup_routes(page, args.url)
                        last_warmup_retry = time.time()
                    _pump_events(page, 250)
            else:
                timeout_seconds = 120
                started = time.time()
                while not should_stop and (time.time() - started) < timeout_seconds:
                    if args.auto_login and not state.login_submitted and (time.time() - last_auto_login_retry >= 5):
                        try:
                            _maybe_auto_login(page, state)
                        except Exception:
                            pass
                        last_auto_login_retry = time.time()
                    if time.time() - last_status_print >= 5:
                        _print_live_status(state)
                        last_status_print = time.time()
                    if args.active_pull:
                        _maybe_prepare_fallback_active_pull(state)
                        _active_pull_transactions(
                            state=state,
                            sheet_writer=live_sheet_writer,
                            min_interval_sec=max(1.0, float(args.pull_interval)),
                        )
                    if (
                        args.auto_warmup
                        and state.login_submitted
                        and not state.active_pull_template_captured
                        and (time.time() - last_warmup_retry >= max(5.0, float(args.warmup_interval)))
                    ):
                        _run_warmup_routes(page, args.url)
                        last_warmup_retry = time.time()
                    _pump_events(page, 250)
        finally:
            if args.active_pull and state.active_pull_template_captured:
                try:
                    _active_pull_transactions(
                        state=state,
                        sheet_writer=live_sheet_writer,
                        min_interval_sec=0.0,
                    )
                except Exception:
                    pass
            try:
                context.close()
            except Exception as exc_close:
                print(f"[warn] context close failed: {exc_close}", file=sys.stderr)

    print("Explorer остановлен. Логи сохранены.")
    _print_live_status(state)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual CAL digital explorer with full network capture")
    parser.add_argument(
        "--profile-dir",
        required=True,
        help="Persistent Chromium profile dir (userDataDir)",
    )
    parser.add_argument("--headful", action="store_true", help="Run browser with GUI")
    parser.add_argument("--keep-open", action="store_true", help="Keep browser open until Ctrl+C")
    parser.add_argument(
        "--log-dir",
        default=str(DEFAULT_LOG_DIR),
        help="Directory for logs/artifacts",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="Start URL")
    parser.add_argument(
        "--auto-login",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Auto-fill login/password from CAL_USERNAME/CAL_PASSWORD on login page",
    )
    parser.add_argument(
        "--capture-contains",
        default=",".join(DEFAULT_CAPTURE_CONTAINS),
        help="Comma-separated URL substrings to capture full response bodies",
    )
    parser.add_argument(
        "--live-sheet",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Push extracted transactions to Google Sheets Journal in near real time",
    )
    parser.add_argument(
        "--sheet-min-interval",
        type=float,
        default=2.0,
        help="Minimum seconds between live Google Sheets pushes",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=90,
        help="Days back window for widened filteredTransactions replay",
    )
    parser.add_argument(
        "--active-pull",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Actively pull filteredTransactions using captured auth/body template",
    )
    parser.add_argument(
        "--pull-interval",
        type=float,
        default=12.0,
        help="Seconds between active pull attempts",
    )
    parser.add_argument(
        "--auto-warmup",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Auto-open transactions routes until filteredTransactions template is captured",
    )
    parser.add_argument(
        "--warmup-interval",
        type=float,
        default=20.0,
        help="Seconds between warmup route attempts",
    )
    parser.add_argument(
        "--fresh-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reset local explorer artifacts before run (csv/md/network/responses)",
    )
    parser.add_argument(
        "--clear-sheet-on-start",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Clear Journal sheet snapshot at startup (requires --live-sheet)",
    )
    parser.add_argument(
        "--seed-request-file",
        default=str(DEFAULT_SEED_REQUEST_PATH),
        help="JSON file with seeded filteredTransactions request: {url, headers, body}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run_explorer(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
