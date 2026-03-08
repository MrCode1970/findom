from __future__ import annotations

import copy
import json
import logging
import re
from datetime import date as date_cls
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests

from tools.connectors._core.errors import InvalidCredentialsError, RateLimitError, TemporaryError
from tools.connectors.providers.cal_digital import discover_api


LOG = logging.getLogger(__name__)
DEFAULT_USER_AGENT = "findom-cal-digital/1.0 (+requests)"
DEFAULT_TIMEOUT = 30
MAX_PAGES = 50
INIT_PATH = "/authentication/api/account/init"
DASHBOARD_PATH = "/transactions/api/lasttransactionsfordashboard/lasttransactionsfordashboard"
CLEARANCE_PATH = "/transactions/api/approvals/getclearancerequests"
FILTERED_TRANSACTIONS_PATH = "/transactions/api/filteredtransactions/getfilteredtransactions"


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _query_dict(url: str) -> dict[str, str]:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in query.items()}


def _strip_query(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _first_str(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_endpoint_hint(endpoint_payload: dict[str, Any]) -> dict[str, Any]:
    sample_post_data = endpoint_payload.get("sample_request_json")
    if sample_post_data is None:
        sample_post_data = endpoint_payload.get("sample_request_post_data")
    if not isinstance(sample_post_data, (dict, list)):
        sample_post_data = None

    header_templates = dict(endpoint_payload.get("header_templates", {}))
    sample_headers = dict(endpoint_payload.get("sample_request_headers", {}))
    for name, value in sample_headers.items():
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        if value == "<redacted>":
            continue
        lower = name.lower()
        if lower in {"x-site-id", "x-requested-with", "origin", "referer", "content-type", "accept"}:
            header_templates.setdefault(name, value)

    return {
        "endpoint_type": endpoint_payload.get("endpoint_type"),
        "method": str(endpoint_payload.get("method", "GET")).upper(),
        "url": str(endpoint_payload.get("url", endpoint_payload.get("url_template", ""))),
        "url_template": str(endpoint_payload.get("url_template", "")),
        "domain": endpoint_payload.get("domain"),
        "path": endpoint_payload.get("path"),
        "query_params": dict(endpoint_payload.get("query_params", {})),
        "required_headers": list(endpoint_payload.get("required_headers", [])),
        "header_templates": header_templates,
        "date_params": list(endpoint_payload.get("date_params", [])),
        "date_fields": list(endpoint_payload.get("date_fields", endpoint_payload.get("date_params", []))),
        "card_params": list(endpoint_payload.get("card_params", [])),
        "pagination_hints": list(endpoint_payload.get("pagination_hints", [])),
        "sample_request_json": sample_post_data,
        "sample_request_post_data": sample_post_data,
        "runtime_headers": {},
        "runtime_request_json": None,
        "array_path": endpoint_payload.get("array_path"),
    }


def _endpoint_path(payload: dict[str, Any]) -> str:
    url = str(payload.get("url") or payload.get("url_template") or "")
    if not url:
        return ""
    return urlsplit(url).path.lower()


def _iter_report_endpoints(report: dict[str, Any]) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    selected = report.get("selected", {})
    for key in ("init", "history", "dashboard", "cards", "transactions"):
        item = selected.get(key)
        if isinstance(item, dict):
            collected.append(item)

    discovered = report.get("discovered", {})
    for key in ("cards", "transactions"):
        items = discovered.get(key, [])
        if isinstance(items, list):
            collected.extend(item for item in items if isinstance(item, dict))
    return collected


def _select_endpoint(
    report: dict[str, Any],
    *,
    endpoint_type: str | None,
    path_contains: str,
) -> dict[str, Any] | None:
    candidates = _iter_report_endpoints(report)
    if endpoint_type:
        typed = [item for item in candidates if item.get("endpoint_type") == endpoint_type]
        if typed:
            candidates = typed

    path_matched = [item for item in candidates if path_contains in _endpoint_path(item)]
    if path_matched:
        candidates = path_matched

    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda item: (int(item.get("score", 0)), int(item.get("event_id", 0))),
        reverse=True,
    )[0]


def _load_runtime_session() -> dict[str, Any]:
    path = discover_api.RUNTIME_SESSION_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _apply_runtime_endpoint_data(endpoint: dict[str, Any], runtime_endpoint: dict[str, Any] | None) -> dict[str, Any]:
    out = copy.deepcopy(endpoint)
    if not isinstance(runtime_endpoint, dict):
        return out

    headers = runtime_endpoint.get("headers")
    if isinstance(headers, dict):
        out["runtime_headers"] = {
            str(name): str(value)
            for name, value in headers.items()
            if isinstance(name, str) and isinstance(value, str) and value
        }
        for name, value in out["runtime_headers"].items():
            out.setdefault("header_templates", {})
            out["header_templates"][name] = value

    request_json = runtime_endpoint.get("request_json")
    if isinstance(request_json, (dict, list)):
        out["runtime_request_json"] = copy.deepcopy(request_json)

    return out


def load_api_hints() -> dict[str, Any]:
    report_path = discover_api.REPORT_JSON_PATH
    if not report_path.exists():
        raise TemporaryError(
            f"CAL discovery report not found: {report_path}. Run discover_api.py first."
        )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    init = _select_endpoint(report, endpoint_type="init", path_contains=INIT_PATH)
    clearance = _select_endpoint(report, endpoint_type=None, path_contains=CLEARANCE_PATH)
    filtered = _select_endpoint(
        report,
        endpoint_type="history",
        path_contains=FILTERED_TRANSACTIONS_PATH,
    )
    if filtered is None:
        filtered = _select_endpoint(
            report,
            endpoint_type=None,
            path_contains=FILTERED_TRANSACTIONS_PATH,
        )
    dashboard = _select_endpoint(report, endpoint_type="dashboard", path_contains=DASHBOARD_PATH)
    runtime_session = _load_runtime_session()
    runtime_endpoints = runtime_session.get("endpoints", {}) if isinstance(runtime_session, dict) else {}

    if not init or not filtered:
        raise TemporaryError(
            "api_report.json does not contain required init/filtered-transactions endpoints"
        )

    init_hint = _apply_runtime_endpoint_data(
        _extract_endpoint_hint(init),
        runtime_endpoints.get("init") if isinstance(runtime_endpoints, dict) else None,
    )
    clearance_hint = None
    if clearance:
        clearance_hint = _apply_runtime_endpoint_data(
            _extract_endpoint_hint(clearance),
            runtime_endpoints.get("clearance") if isinstance(runtime_endpoints, dict) else None,
        )
    filtered_hint = _apply_runtime_endpoint_data(
        _extract_endpoint_hint(filtered),
        runtime_endpoints.get("history") if isinstance(runtime_endpoints, dict) else None,
    )
    dashboard_hint = None
    if dashboard:
        dashboard_hint = _apply_runtime_endpoint_data(
            _extract_endpoint_hint(dashboard),
            runtime_endpoints.get("dashboard") if isinstance(runtime_endpoints, dict) else None,
        )

    hints = {
        "base_url": report.get("base_url", discover_api.DEFAULT_BASE_URL),
        "init": init_hint,
        "clearance": clearance_hint,
        "filtered_transactions": filtered_hint,
        "history": filtered_hint,
        "dashboard": dashboard_hint,
        "cards": init_hint,
        "transactions": filtered_hint,
        "storage_state_path": str(discover_api.STORAGE_STATE_PATH),
        "runtime_session_path": str(discover_api.RUNTIME_SESSION_PATH),
        "report_generated_at": report.get("generated_at"),
    }
    return hints


def load_cookies_from_storage_state(storage_state_path: str | Path) -> requests.cookies.RequestsCookieJar:
    path = Path(storage_state_path)
    if not path.exists():
        raise TemporaryError(f"Storage state not found: {path}")

    state = json.loads(path.read_text(encoding="utf-8"))
    jar = requests.cookies.RequestsCookieJar()

    for cookie in state.get("cookies", []):
        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None:
            continue
        jar.set(
            name,
            value,
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )

    return jar


def _load_storage_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise TemporaryError(f"Storage state not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _apply_state_cookies(
    jar: requests.cookies.RequestsCookieJar,
    cookies: Any,
) -> requests.cookies.RequestsCookieJar:
    if isinstance(cookies, dict):
        for name, value in cookies.items():
            if name and value is not None:
                jar.set(str(name), str(value))
        return jar

    if isinstance(cookies, list):
        for item in cookies:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            value = item.get("value")
            if not name or value is None:
                continue
            jar.set(
                str(name),
                str(value),
                domain=item.get("domain"),
                path=item.get("path", "/"),
            )

    return jar


def _extract_storage_tokens(state: dict[str, Any]) -> dict[str, str]:
    tokens: dict[str, str] = {}
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            name = str(item.get("name", ""))
            value = item.get("value")
            if isinstance(value, str) and value:
                tokens[name] = value
    return tokens


def _pick_token(tokens: dict[str, str], markers: tuple[str, ...]) -> str | None:
    lowered = {name.lower(): value for name, value in tokens.items()}
    for key, value in lowered.items():
        if any(marker in key for marker in markers):
            return value
    return None


def _header_value_from_storage(header_name: str, tokens: dict[str, str]) -> str | None:
    lower = header_name.lower()
    if "csrf" in lower or "xsrf" in lower:
        return _pick_token(tokens, ("csrf", "xsrf"))

    if "authorization" in lower or "auth" in lower:
        return _pick_token(
            tokens,
            ("authorization", "access_token", "accesstoken", "id_token", "jwt", "auth", "token"),
        )

    if "token" in lower:
        return _pick_token(tokens, ("token", "access", "auth", "jwt"))

    return None


def _merge_header_templates(api_hints: dict[str, Any]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for section in ("init", "history", "dashboard"):
        endpoint = api_hints.get(section, {})
        if not isinstance(endpoint, dict):
            continue
        for key, value in endpoint.get("header_templates", {}).items():
            if isinstance(key, str) and isinstance(value, str):
                merged[key] = value
    return merged


def _required_header_names(api_hints: dict[str, Any]) -> list[str]:
    names: set[str] = set()
    for section in ("init", "history", "dashboard"):
        endpoint = api_hints.get(section, {})
        if not isinstance(endpoint, dict):
            continue
        for key in endpoint.get("required_headers", []):
            if isinstance(key, str) and key:
                names.add(key)
    return sorted(names)


def build_session(state: dict[str, Any], api_hints: dict[str, Any]) -> requests.Session:
    storage_state_path = state.get("storage_state_path") or api_hints.get("storage_state_path")
    storage_state: dict[str, Any] = {"cookies": [], "origins": []}
    storage_path = Path(storage_state_path) if storage_state_path else None
    if storage_path and storage_path.exists():
        storage_state = _load_storage_state(storage_path)

    session = requests.Session()

    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    if storage_path and storage_path.exists():
        session.cookies.update(load_cookies_from_storage_state(storage_path))
    elif state.get("cookies"):
        session.cookies.update(_apply_state_cookies(requests.cookies.RequestsCookieJar(), state["cookies"]))
    else:
        raise TemporaryError(
            "No usable CAL session source: storage_state_path/cookies are missing"
        )

    header_templates = _merge_header_templates(api_hints)
    for name, value in header_templates.items():
        session.headers[name] = value

    storage_tokens = _extract_storage_tokens(storage_state)
    setattr(session, "_findom_storage_tokens", storage_tokens)
    for header_name in _required_header_names(api_hints):
        if header_name in session.headers:
            continue
        value = _header_value_from_storage(header_name, storage_tokens)
        if value:
            session.headers[header_name] = value

    LOG.info(
        "Built CAL session with %s cookies and %s headers",
        len(session.cookies),
        len(session.headers),
    )
    return session


def _token_for_body_field(field_name: str, storage_tokens: dict[str, str]) -> str | None:
    normalized = _normalize_key(field_name)
    if "tokenguid" in normalized:
        return _pick_token(storage_tokens, ("tokenguid", "token_guid", "guid"))
    if "csrf" in normalized or "xsrf" in normalized:
        return _pick_token(storage_tokens, ("csrf", "xsrf"))
    if "token" in normalized or "auth" in normalized:
        return _pick_token(
            storage_tokens,
            ("authorization", "access_token", "accesstoken", "id_token", "jwt", "auth", "token"),
        )
    return None


def _hydrate_redacted_body_fields(value: Any, storage_tokens: dict[str, str]) -> Any:
    if isinstance(value, dict):
        hydrated: dict[str, Any] = {}
        for key, sub_value in value.items():
            if sub_value == "<redacted>":
                replacement = _token_for_body_field(key, storage_tokens)
                hydrated[key] = replacement if replacement is not None else sub_value
            else:
                hydrated[key] = _hydrate_redacted_body_fields(sub_value, storage_tokens)
        return hydrated
    if isinstance(value, list):
        return [_hydrate_redacted_body_fields(item, storage_tokens) for item in value]
    return value


def _raise_for_status(status: int) -> None:
    if status in (401, 403):
        raise InvalidCredentialsError(f"CAL API returned status={status}")
    if status == 429:
        raise RateLimitError("CAL API rate limited request")
    if status >= 500:
        raise TemporaryError(f"CAL API temporary error status={status}")
    if status >= 400:
        raise TemporaryError(f"CAL API unexpected status={status}")


def _parse_response_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        raise TemporaryError("CAL API returned non-JSON response") from exc


def _is_from_date_key(name: str) -> bool:
    normalized = _normalize_key(name)
    return ("from" in normalized and "date" in normalized) or normalized in {
        "fromdate",
        "startdate",
        "mindate",
        "datefrom",
    }


def _is_to_date_key(name: str) -> bool:
    normalized = _normalize_key(name)
    return ("to" in normalized and "date" in normalized) or normalized in {
        "todate",
        "enddate",
        "maxdate",
        "dateto",
    }


def _leaf_key(name: str) -> str:
    leaf = name.split(".")[-1]
    if "[" in leaf:
        leaf = leaf.split("[", 1)[0]
    return leaf


def _format_date_like_sample(sample_value: Any, date_value: str) -> str:
    if isinstance(sample_value, str):
        if "T" in sample_value and sample_value.endswith("Z"):
            return f"{date_value}T00:00:00.000Z"
        if "T" in sample_value:
            return f"{date_value}T00:00:00+00:00"
    return date_value


def _to_utc_iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _history_day_window_utc(from_date: str, to_date: str) -> tuple[str, str]:
    try:
        from_day = date_cls.fromisoformat(from_date)
        to_day = date_cls.fromisoformat(to_date)
    except ValueError:
        return from_date, to_date

    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    start_local = datetime.combine(from_day, time(0, 0, 0, 0), tzinfo=local_tz)
    end_local = datetime.combine(to_day, time(23, 59, 59, 999000), tzinfo=local_tz)
    return _to_utc_iso_z(start_local), _to_utc_iso_z(end_local)


def _normalized_date_field_names(endpoint: dict[str, Any]) -> set[str]:
    names = endpoint.get("date_fields") or endpoint.get("date_params") or []
    return {_normalize_key(_leaf_key(str(name))) for name in names if str(name).strip()}


def _replace_date_fields_in_json(
    value: Any,
    *,
    normalized_date_fields: set[str],
    from_date: str,
    to_date: str,
) -> Any:
    if isinstance(value, dict):
        updated: dict[str, Any] = {}
        for key, sub_value in value.items():
            normalized = _normalize_key(key)
            if normalized in normalized_date_fields and _is_from_date_key(key):
                updated[key] = _format_date_like_sample(sub_value, from_date)
            elif normalized in normalized_date_fields and _is_to_date_key(key):
                updated[key] = _format_date_like_sample(sub_value, to_date)
            elif normalized in normalized_date_fields and "date" in normalized:
                updated[key] = _format_date_like_sample(sub_value, to_date)
            else:
                updated[key] = _replace_date_fields_in_json(
                    sub_value,
                    normalized_date_fields=normalized_date_fields,
                    from_date=from_date,
                    to_date=to_date,
                )
        return updated

    if isinstance(value, list):
        return [
            _replace_date_fields_in_json(
                item,
                normalized_date_fields=normalized_date_fields,
                from_date=from_date,
                to_date=to_date,
            )
            for item in value
        ]

    return value


def extract_init_context(init_payload: Any) -> dict[str, Any]:
    context: dict[str, Any] = {"bank_account_unique_id": None, "card_unique_ids": []}
    if not isinstance(init_payload, dict):
        return context

    payload = init_payload.get("result") if isinstance(init_payload.get("result"), dict) else init_payload
    if not isinstance(payload, dict):
        return context

    bank_accounts = payload.get("bankAccounts")
    if isinstance(bank_accounts, list):
        for account in bank_accounts:
            if not isinstance(account, dict):
                continue
            for key, value in account.items():
                normalized = _normalize_key(key)
                if normalized == "bankaccountuniqueid" and value not in (None, ""):
                    context["bank_account_unique_id"] = str(value)
                    break
            if context["bank_account_unique_id"]:
                break

    cards = payload.get("cards")
    card_ids: list[str] = []
    if isinstance(cards, list):
        for card in cards:
            if not isinstance(card, dict):
                continue
            for key, value in card.items():
                normalized = _normalize_key(key)
                if "card" in normalized and "uniqueid" in normalized and value not in (None, ""):
                    card_id = str(value)
                    if card_id not in card_ids:
                        card_ids.append(card_id)
                    break
    context["card_unique_ids"] = card_ids
    return context


def _apply_init_context_to_history_body(body: Any, init_context: dict[str, Any] | None) -> Any:
    if not isinstance(body, dict) or not init_context:
        return body

    updated = copy.deepcopy(body)
    bank_account_unique_id = init_context.get("bank_account_unique_id")
    if bank_account_unique_id:
        for key in list(updated.keys()):
            if _normalize_key(key) == "bankaccountuniqueid":
                updated[key] = bank_account_unique_id

    card_ids = init_context.get("card_unique_ids") or []
    if card_ids and isinstance(updated.get("cards"), list):
        cards_sample = updated.get("cards") or []
        card_key = "cardUniqueID"
        if cards_sample and isinstance(cards_sample[0], dict):
            for existing_key in cards_sample[0].keys():
                normalized = _normalize_key(existing_key)
                if "card" in normalized and "id" in normalized:
                    card_key = existing_key
                    break
        updated["cards"] = [{card_key: card_id} for card_id in card_ids]

    return updated


def _is_filtered_transactions_endpoint(endpoint: dict[str, Any]) -> bool:
    return FILTERED_TRANSACTIONS_PATH in _endpoint_path(endpoint)


def _apply_filtered_history_profile(
    body: Any,
    *,
    endpoint: dict[str, Any],
    init_context: dict[str, Any] | None,
    from_date: str,
    to_date: str,
) -> Any:
    if not isinstance(body, dict) or not _is_filtered_transactions_endpoint(endpoint):
        return body

    updated = copy.deepcopy(body)
    from_utc, to_utc = _history_day_window_utc(from_date, to_date)

    updated["fromTransDate"] = from_utc
    updated["toTransDate"] = to_utc
    updated["merchantHebName"] = ""
    updated["merchantHebCity"] = ""
    updated["trnType"] = 0
    updated["fromTrnAmt"] = 0
    updated["toTrnAmt"] = 0
    updated["transactionsOrigin"] = 0
    updated["transCardPresentInd"] = 0
    updated["caller"] = "module_search"
    # UI search-pattern request does not include walletTranInd.
    updated.pop("walletTranInd", None)

    if isinstance(init_context, dict):
        bank_account_unique_id = init_context.get("bank_account_unique_id")
        if bank_account_unique_id:
            updated["bankAccountUniqueID"] = str(bank_account_unique_id)

        card_ids = [str(item) for item in (init_context.get("card_unique_ids") or []) if str(item).strip()]
        if card_ids:
            updated["cards"] = [{"cardUniqueID": card_id} for card_id in card_ids]

    return updated


def build_history_request_body(
    endpoint: dict[str, Any],
    *,
    from_date: str,
    to_date: str,
    init_context: dict[str, Any] | None = None,
) -> Any | None:
    sample = endpoint.get("sample_request_json")
    body = copy.deepcopy(sample) if isinstance(sample, (dict, list)) else None
    if body is None:
        return None

    body = _replace_date_fields_in_json(
        body,
        normalized_date_fields=_normalized_date_field_names(endpoint),
        from_date=from_date,
        to_date=to_date,
    )
    body = _apply_init_context_to_history_body(body, init_context)
    return _apply_filtered_history_profile(
        body,
        endpoint=endpoint,
        init_context=init_context,
        from_date=from_date,
        to_date=to_date,
    )


def build_history_query_params(
    endpoint: dict[str, Any],
    *,
    from_date: str,
    to_date: str,
) -> dict[str, str]:
    raw_url = endpoint.get("url_template") or endpoint.get("url") or ""
    params = _query_dict(raw_url)
    params.update(endpoint.get("query_params", {}))
    normalized_date_fields = _normalized_date_field_names(endpoint)
    for key, value in list(params.items()):
        normalized = _normalize_key(_leaf_key(key))
        if normalized not in normalized_date_fields:
            continue
        if _is_from_date_key(key):
            params[key] = _format_date_like_sample(value, from_date)
        elif _is_to_date_key(key):
            params[key] = _format_date_like_sample(value, to_date)
        elif "date" in normalized:
            params[key] = _format_date_like_sample(value, to_date)
    return params


def _set_recursive_key(data: Any, key_name: str, new_value: Any) -> None:
    if isinstance(data, dict):
        for key, value in data.items():
            if _normalize_key(key) == _normalize_key(key_name):
                data[key] = new_value
            else:
                _set_recursive_key(value, key_name, new_value)
    elif isinstance(data, list):
        for item in data:
            _set_recursive_key(item, key_name, new_value)


def _path_tokens(path: str) -> list[str]:
    cleaned = path.strip()
    if not cleaned or cleaned == "$":
        return []
    if cleaned.startswith("$."):
        cleaned = cleaned[2:]
    elif cleaned.startswith("$"):
        cleaned = cleaned[1:]
    return [token for token in cleaned.split(".") if token]


def _extract_path(data: Any, path: str | None) -> Any:
    if not path:
        return data

    current = data
    for token in _path_tokens(path):
        if "[" in token and token.endswith("]"):
            name, index_part = token.split("[", 1)
            index = int(index_part[:-1])
            if name:
                if not isinstance(current, dict):
                    return None
                current = current.get(name)
            if not isinstance(current, list) or index >= len(current):
                return None
            current = current[index]
            continue

        if not isinstance(current, dict):
            return None
        current = current.get(token)

    return current


def _find_list_fallback(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        items = [item for item in value if isinstance(item, dict)]
        if items:
            return items
        return None

    if isinstance(value, dict):
        for sub_value in value.values():
            found = _find_list_fallback(sub_value)
            if found:
                return found

    return None


def _extract_items(payload: Any, array_path: str | None) -> list[dict[str, Any]]:
    extracted = _extract_path(payload, array_path)
    if isinstance(extracted, list):
        return [item for item in extracted if isinstance(item, dict)]

    fallback = _find_list_fallback(payload)
    return fallback or []


def _pick_pagination_keys(endpoint: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    page_key = None
    offset_key = None
    limit_key = None

    for hint in endpoint.get("pagination_hints", []):
        normalized = _normalize_key(hint)
        if page_key is None and "page" in normalized and "size" not in normalized:
            page_key = hint
            continue
        if offset_key is None and "offset" in normalized:
            offset_key = hint
            continue
        if limit_key is None and any(marker in normalized for marker in ("limit", "size", "pagesize")):
            limit_key = hint

    return page_key, offset_key, limit_key


def _set_pagination(
    params: dict[str, str],
    body: Any | None,
    *,
    page_key: str | None,
    page_value: int,
    offset_key: str | None,
    offset_value: int,
    limit_key: str | None,
    limit_value: int,
) -> tuple[dict[str, str], Any | None]:
    updated_params = dict(params)
    updated_body = copy.deepcopy(body)

    if page_key:
        updated_params[page_key] = str(page_value)
        if updated_body is not None:
            _set_recursive_key(updated_body, page_key, page_value)

    if offset_key:
        updated_params[offset_key] = str(offset_value)
        if updated_body is not None:
            _set_recursive_key(updated_body, offset_key, offset_value)

    if limit_key:
        updated_params[limit_key] = str(limit_value)
        if updated_body is not None:
            _set_recursive_key(updated_body, limit_key, limit_value)

    return updated_params, updated_body


def _request_json(
    session: requests.Session,
    endpoint: dict[str, Any],
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    page: int | None = None,
    offset: int | None = None,
    limit: int | None = None,
    init_context: dict[str, Any] | None = None,
) -> Any:
    raw_url = endpoint.get("url") or endpoint.get("url_template")
    if not raw_url:
        raise TemporaryError("Endpoint URL is missing in api_hints")

    default_params = _query_dict(endpoint.get("url_template") or raw_url)
    default_params.update(endpoint.get("query_params", {}))
    runtime_body = endpoint.get("runtime_request_json")
    if isinstance(runtime_body, (dict, list)):
        body = copy.deepcopy(runtime_body)
    else:
        body = copy.deepcopy(endpoint.get("sample_request_json"))
    params = dict(default_params)

    if endpoint.get("endpoint_type") == "history":
        if not from_date or not to_date:
            raise TemporaryError("History endpoint requires from_date/to_date")
        params = build_history_query_params(
            endpoint,
            from_date=from_date,
            to_date=to_date,
        )
        body = build_history_request_body(
            endpoint,
            from_date=from_date,
            to_date=to_date,
            init_context=init_context,
        )

    storage_tokens = getattr(session, "_findom_storage_tokens", {})
    if isinstance(storage_tokens, dict) and body is not None:
        body = _hydrate_redacted_body_fields(body, storage_tokens)

    if page is not None or offset is not None or limit is not None:
        page_key, offset_key, limit_key = _pick_pagination_keys(endpoint)
        params, body = _set_pagination(
            params,
            body,
            page_key=page_key,
            page_value=page or 1,
            offset_key=offset_key,
            offset_value=offset or 0,
            limit_key=limit_key,
            limit_value=limit or 100,
        )

    method = str(endpoint.get("method", "GET")).upper()
    url = _strip_query(raw_url)

    kwargs: dict[str, Any] = {
        "params": params,
        "timeout": DEFAULT_TIMEOUT,
    }
    if method in {"POST", "PUT", "PATCH", "DELETE"} and body is not None:
        kwargs["json"] = body

    LOG.info("CAL request %s %s", method, url)
    try:
        response = session.request(method, url, **kwargs)
    except requests.RequestException as exc:
        raise TemporaryError(f"CAL request failed: {exc}") from exc

    _raise_for_status(response.status_code)
    return _parse_response_json(response)


def fetch_cards(session: requests.Session, api_hints: dict[str, Any]) -> dict | list:
    payload = _request_json(
        session,
        api_hints["init"],
    )
    if not isinstance(payload, (dict, list)):
        raise TemporaryError("Cards endpoint returned unsupported payload type")
    return payload


def _fetch_history_endpoint_transactions(
    session: requests.Session,
    endpoint: dict[str, Any],
    card_id: str | None,
    from_date: str,
    to_date: str,
    init_payload: Any | None = None,
) -> list[dict[str, Any]]:
    page_key, offset_key, limit_key = _pick_pagination_keys(endpoint)
    init_context = extract_init_context(init_payload)
    if card_id and card_id in set(init_context.get("card_unique_ids", [])):
        init_context = dict(init_context)
        init_context["card_unique_ids"] = [card_id]

    pagination_enabled = bool(page_key or offset_key or limit_key)
    page = 1
    offset = 0
    limit_value = 100

    default_limit = _first_str(endpoint.get("query_params", {}), (limit_key,) if limit_key else tuple())
    if default_limit and default_limit.isdigit():
        limit_value = int(default_limit)

    all_items: list[dict[str, Any]] = []
    for _ in range(MAX_PAGES):
        payload = _request_json(
            session,
            endpoint,
            from_date=from_date,
            to_date=to_date,
            page=page if pagination_enabled else None,
            offset=offset if pagination_enabled else None,
            limit=limit_value if pagination_enabled else None,
            init_context=init_context,
        )
        items = _extract_items(payload, endpoint.get("array_path"))
        if not items:
            break

        all_items.extend(items)
        if not pagination_enabled:
            break

        if limit_key and len(items) < limit_value:
            break

        if page_key:
            page += 1
        if offset_key:
            offset += len(items)

        if not page_key and not offset_key:
            break

    return all_items


def fetch_clearance_requests(
    session: requests.Session,
    api_hints: dict[str, Any],
    card_id: str | None,
    from_date: str,
    to_date: str,
    init_payload: Any | None = None,
) -> list[dict[str, Any]]:
    endpoint = api_hints.get("clearance")
    if not isinstance(endpoint, dict):
        return []
    return _fetch_history_endpoint_transactions(
        session,
        endpoint,
        card_id=card_id,
        from_date=from_date,
        to_date=to_date,
        init_payload=init_payload,
    )


def fetch_filtered_transactions(
    session: requests.Session,
    api_hints: dict[str, Any],
    card_id: str | None,
    from_date: str,
    to_date: str,
    init_payload: Any | None = None,
) -> list[dict[str, Any]]:
    endpoint = api_hints.get("filtered_transactions") or api_hints.get("history")
    if not isinstance(endpoint, dict):
        raise TemporaryError("Filtered-transactions endpoint is missing in api_hints")
    return _fetch_history_endpoint_transactions(
        session,
        endpoint,
        card_id=card_id,
        from_date=from_date,
        to_date=to_date,
        init_payload=init_payload,
    )


def fetch_transactions(
    session: requests.Session,
    api_hints: dict[str, Any],
    card_id: str | None,
    from_date: str,
    to_date: str,
    init_payload: Any | None = None,
) -> list[dict[str, Any]]:
    return fetch_filtered_transactions(
        session,
        api_hints,
        card_id=card_id,
        from_date=from_date,
        to_date=to_date,
        init_payload=init_payload,
    )
