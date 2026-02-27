from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import sys
import textwrap
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests


DEFAULT_BASE_URL = "https://digital-web.cal-online.co.il/"
MAX_JSON_PREVIEW_BYTES = 200 * 1024
OUTPUT_ROOT = Path(__file__).resolve().parents[4] / ".state" / "cal_digital" / "api_discovery"
DEFAULT_DOTENV_PATH = ".env"
PRIMARY_USERNAME_ENV = "CAL_USERNAME"
PRIMARY_PASSWORD_ENV = "CAL_PASSWORD"
FALLBACK_USERNAME_ENVS = ("FINDOM_CAL_USERNAME",)
FALLBACK_PASSWORD_ENVS = ("FINDOM_CAL_PASSWORD",)

NETWORK_LOG_PATH = OUTPUT_ROOT / "network_log.jsonl"
REPORT_JSON_PATH = OUTPUT_ROOT / "api_report.json"
REPORT_MD_PATH = OUTPUT_ROOT / "api_report.md"
REQUESTS_SAMPLES_PATH = OUTPUT_ROOT / "requests_samples.py"
STORAGE_STATE_PATH = OUTPUT_ROOT / "storage_state.json"
DIAGNOSTICS_PATH = OUTPUT_ROOT / "diagnostics.json"
RUNTIME_SESSION_PATH = OUTPUT_ROOT / "runtime_session.json"

VOLATILE_QUERY_KEYS = {
    "_",
    "cb",
    "cachebuster",
    "nonce",
    "requestid",
    "rnd",
    "ts",
    "timestamp",
}

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
    "password",
    "token",
    "secret",
    "cookie",
    "csrf",
)

CARD_KEY_HINTS = (
    "cardid",
    "cardnumbermasked",
    "maskedcard",
    "last4",
    "cardname",
    "cardnumber",
    "pan",
)
CARD_ID_HINTS = (
    "cardid",
    "card_id",
    "cardnumber",
    "pan",
    "id",
)
CARD_PARAM_HINTS = (
    "cardid",
    "card_id",
    "cardnumber",
    "carduniqueid",
    "cards",
    "pan",
    "bankaccountuniqueid",
)
DATE_KEY_HINTS = (
    "date",
    "txn",
    "transactiondate",
    "bookingdate",
    "valuedate",
    "eventdate",
)
AMOUNT_KEY_HINTS = (
    "amount",
    "sum",
    "value",
    "total",
    "debit",
    "credit",
)
MERCHANT_KEY_HINTS = (
    "merchant",
    "business",
    "description",
    "store",
    "mcc",
    "name",
)
PAGINATION_KEY_HINTS = ("page", "offset", "limit", "cursor", "pagesize", "pageindex")
FROM_DATE_HINTS = ("from", "start", "mindate", "datefrom")
TO_DATE_HINTS = ("to", "end", "maxdate", "dateto")
ALLOWED_PAGE_HOSTS = (
    "digital-web.cal-online.co.il",
    "connect.cal-online.co.il",
)
FORBIDDEN_NAV_MARKERS = (
    "עיצוב",
    "מעוצב",
    "design",
)
FORBIDDEN_LOGIN_MARKERS = (
    "בית עסק",
    "לעסק",
    "עסקי",
    "business",
)
KNOWN_TX_ENDPOINTS = (
    "https://api.cal-online.co.il/Transactions/api/transactionsDetails/getCardTransactionsDetails",
    "https://api.cal-online.co.il/Transactions/api/transactionsDetails/getCardTransactionsDetailsV2",
)
INIT_ENDPOINT_PATH = "/authentication/api/account/init"
HISTORY_ENDPOINT_PATH = "/transactions/api/filteredtransactions/getfilteredtransactions"
DASHBOARD_ENDPOINT_PATH = "/transactions/api/lasttransactionsfordashboard/lasttransactionsfordashboard"
TX_WARMUP_PATHS = (
    "/transactions",
    "/transactions-and-debits",
    "/transactions-search",
    "/search-transactions",
    "/bank-debits",
)


@dataclass(slots=True)
class CapturedEvent:
    event_id: int
    method: str
    url: str
    query_params: dict[str, str]
    request_headers: dict[str, str]
    request_body_json: Any | None
    request_cookie_names: list[str]
    response_status: int
    response_headers: dict[str, str]
    response_json: Any | None


@dataclass(slots=True)
class EndpointCandidate:
    kind: str
    endpoint_type: str
    event_id: int
    score: int
    method: str
    url: str
    url_template: str
    query_params: dict[str, str]
    request_headers: dict[str, str]
    request_body_json: Any | None
    request_cookie_names: list[str]
    array_path: str
    matched_keys: list[str]
    pagination_hints: list[str]
    date_params: list[str]
    card_params: list[str]
    sample_response_shape: list[str]
    required_headers: list[str] = field(default_factory=list)
    header_templates: dict[str, str] = field(default_factory=dict)
    replay: dict[str, Any] = field(default_factory=dict)


def _configure_logging(debug: bool) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("cal_digital.discovery")
    logger.setLevel(level)
    return logger


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = _strip_quotes(value.strip())

    return values


def _dotenv_candidates(dotenv_path: str) -> list[Path]:
    path = Path(dotenv_path).expanduser()
    if path.is_absolute():
        return [path]

    cwd_path = Path.cwd() / path
    repo_path = Path(__file__).resolve().parents[4] / path
    if cwd_path == repo_path:
        return [cwd_path]
    return [cwd_path, repo_path]


def _read_credential(
    names: Iterable[str],
    dotenv_values: dict[str, str],
) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    for name in names:
        value = dotenv_values.get(name)
        if value:
            return value
    return None


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _is_sensitive_header(name: str) -> bool:
    lower = name.lower()
    return any(marker in lower for marker in SENSITIVE_HEADER_MARKERS)


def _is_sensitive_field(name: str) -> bool:
    lower = name.lower()
    return any(marker in lower for marker in SENSITIVE_FIELD_MARKERS)


def _mask_headers(headers: dict[str, str]) -> dict[str, str]:
    masked: dict[str, str] = {}
    for name, value in headers.items():
        if _is_sensitive_header(name):
            masked[name] = "<redacted>"
        else:
            masked[name] = value
    return masked


def _sanitize_json(value: Any, parent_key: str = "") -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, sub_value in value.items():
            if _is_sensitive_field(key):
                sanitized[key] = "<redacted>"
            else:
                sanitized[key] = _sanitize_json(sub_value, key)
        return sanitized

    if isinstance(value, list):
        return [_sanitize_json(item, parent_key) for item in value[:200]]

    if isinstance(value, str) and _is_sensitive_field(parent_key):
        return "<redacted>"

    return value


def _truncate_text_bytes(text: str, limit_bytes: int) -> tuple[str, bool]:
    encoded = text.encode("utf-8")
    if len(encoded) <= limit_bytes:
        return text, False

    preview = encoded[:limit_bytes]
    return preview.decode("utf-8", errors="ignore"), True


def _safe_parse_json(text: str | None) -> Any | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _query_params_from_url(url: str) -> dict[str, str]:
    parsed = urlsplit(url)
    parsed_params = parse_qs(parsed.query, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed_params.items()}


def _filter_volatile_params(params: dict[str, str]) -> dict[str, str]:
    filtered: dict[str, str] = {}
    for key, value in params.items():
        normalized = _normalize_key(key)
        if normalized in VOLATILE_QUERY_KEYS:
            continue
        filtered[key] = value
    return filtered


def _url_template(url: str) -> str:
    parsed = urlsplit(url)
    params = _query_params_from_url(url)
    filtered_params = _filter_volatile_params(params)

    if filtered_params:
        query = "&".join(f"{key}={value}" for key, value in sorted(filtered_params.items()))
    else:
        query = ""

    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _cookie_names_from_header(cookie_header: str | None) -> list[str]:
    if not cookie_header:
        return []

    names: list[str] = []
    for chunk in cookie_header.split(";"):
        part = chunk.strip()
        if not part:
            continue
        name, _, _ = part.partition("=")
        if name:
            names.append(name)
    return sorted(set(names))


def _iter_object_arrays(data: Any, path: str = "$", depth: int = 0, max_depth: int = 6):
    if depth > max_depth:
        return

    if isinstance(data, list):
        dict_items = [item for item in data[:20] if isinstance(item, dict)]
        if dict_items:
            yield path, dict_items

        for index, item in enumerate(data[:20]):
            yield from _iter_object_arrays(item, f"{path}[{index}]", depth + 1, max_depth)
        return

    if isinstance(data, dict):
        for key, value in list(data.items())[:80]:
            yield from _iter_object_arrays(value, f"{path}.{key}", depth + 1, max_depth)


def _card_score(keys: set[str]) -> int:
    explicit = sum(1 for hint in CARD_KEY_HINTS if any(hint in key for key in keys))
    generic_card_keys = sum(1 for key in keys if "card" in key)
    return explicit + min(2, generic_card_keys)


def _transaction_score(keys: set[str]) -> int:
    date_hit = any(any(hint in key for hint in DATE_KEY_HINTS) for key in keys)
    amount_hit = any(any(hint in key for hint in AMOUNT_KEY_HINTS) for key in keys)
    merchant_hit = any(any(hint in key for hint in MERCHANT_KEY_HINTS) for key in keys)
    if not date_hit:
        return 0
    return int(date_hit) + int(amount_hit) + int(merchant_hit)


def _flatten_json_keys(value: Any, prefix: str = "", depth: int = 0, max_depth: int = 5) -> list[str]:
    if depth > max_depth:
        return []

    result: list[str] = []
    if isinstance(value, dict):
        for key, sub_value in value.items():
            path = f"{prefix}.{key}" if prefix else key
            result.append(path)
            result.extend(_flatten_json_keys(sub_value, path, depth + 1, max_depth))
    elif isinstance(value, list):
        for index, item in enumerate(value[:10]):
            path = f"{prefix}[{index}]" if prefix else f"[{index}]"
            result.extend(_flatten_json_keys(item, path, depth + 1, max_depth))
    return result


def _leaf_key(name: str) -> str:
    leaf = name.split(".")[-1]
    if "[" in leaf:
        leaf = leaf.split("[", 1)[0]
    return leaf


def _is_date_field_name(name: str) -> bool:
    normalized = _normalize_key(_leaf_key(name))
    return "date" in normalized


def _is_card_param_name(name: str) -> bool:
    normalized = _normalize_key(_leaf_key(name))
    if "tokenguid" in normalized:
        return False
    if "card" in normalized:
        return True
    return any(hint in normalized for hint in CARD_PARAM_HINTS)


def _top_level_shape_keys(value: Any) -> list[str]:
    if isinstance(value, dict):
        return sorted(str(key) for key in value.keys())
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            return ["[]"] + sorted(str(key) for key in value[0].keys())
        return ["[]"]
    return []


def _detect_hints(query_params: dict[str, str], body_json: Any | None) -> tuple[list[str], list[str], list[str]]:
    names: set[str] = set(query_params.keys())
    names.update(_flatten_json_keys(body_json))

    pagination_hints = sorted(
        name for name in names if any(hint in _normalize_key(name) for hint in PAGINATION_KEY_HINTS)
    )
    date_params = sorted(name for name in names if _is_date_field_name(name))
    card_params = sorted(
        name for name in names if _is_card_param_name(name)
    )
    return pagination_hints, date_params, card_params


def _pick_header_templates(headers: dict[str, str]) -> dict[str, str]:
    templates: dict[str, str] = {}
    for name, value in headers.items():
        lower = name.lower()
        if lower in {"accept", "content-type", "origin", "referer", "x-requested-with"}:
            templates[name] = value
    return templates


def _classify_endpoint_type(event: CapturedEvent, kind: str, keys: set[str]) -> str:
    path = urlsplit(event.url).path.lower()
    normalized_keys = {_normalize_key(key) for key in keys}

    if INIT_ENDPOINT_PATH in path:
        return "init"

    if HISTORY_ENDPOINT_PATH in path:
        return "history"

    if DASHBOARD_ENDPOINT_PATH in path:
        return "dashboard"

    if kind == "transactions":
        has_txn_keys = any(
            any(hint in key for hint in DATE_KEY_HINTS) for key in normalized_keys
        ) and any(any(hint in key for hint in AMOUNT_KEY_HINTS) for key in normalized_keys)
        if has_txn_keys:
            has_request_dates = bool(_detect_hints(event.query_params, event.request_body_json)[1])
            return "history" if has_request_dates else "dashboard"

    if any("tokenguid" in key or "bankaccountuniqueid" in key for key in normalized_keys):
        return "init"

    return "unknown"


def _candidate_to_public(candidate: EndpointCandidate) -> dict[str, Any]:
    parsed = urlsplit(candidate.url_template or candidate.url)
    return {
        "kind": candidate.kind,
        "endpoint_type": candidate.endpoint_type,
        "event_id": candidate.event_id,
        "score": candidate.score,
        "method": candidate.method,
        "domain": parsed.netloc,
        "path": parsed.path,
        "url": candidate.url,
        "url_template": candidate.url_template,
        "query_params": _filter_volatile_params(candidate.query_params),
        "required_headers": candidate.required_headers,
        "header_templates": candidate.header_templates,
        "required_cookies": candidate.request_cookie_names,
        "pagination_hints": candidate.pagination_hints,
        "date_params": candidate.date_params,
        "date_fields": candidate.date_params,
        "card_params": candidate.card_params,
        "matched_keys": candidate.matched_keys,
        "array_path": candidate.array_path,
        "sample_request_headers": _mask_headers(candidate.request_headers),
        "sample_request_json": _sanitize_json(candidate.request_body_json),
        "sample_request_post_data": _sanitize_json(candidate.request_body_json),
        "sample_response_shape": candidate.sample_response_shape,
        "replay": candidate.replay,
    }


def _discover_candidates(events: list[CapturedEvent], kind: str) -> list[EndpointCandidate]:
    candidates: list[EndpointCandidate] = []

    for event in events:
        if event.response_status >= 500 or event.response_json is None:
            continue

        best_score = -1
        best_path = ""
        best_keys: set[str] = set()

        for path, dict_items in _iter_object_arrays(event.response_json):
            keys: set[str] = set()
            for item in dict_items[:20]:
                keys.update(_normalize_key(key) for key in item.keys())

            if not keys:
                continue

            score = _card_score(keys) if kind == "cards" else _transaction_score(keys)
            threshold = 2
            if score < threshold:
                continue

            if score > best_score:
                best_score = score
                best_path = path
                best_keys = keys

        if best_score < 0:
            continue

        pagination_hints, date_params, card_params = _detect_hints(
            event.query_params,
            event.request_body_json,
        )

        candidate = EndpointCandidate(
            kind=kind,
            endpoint_type=_classify_endpoint_type(event, kind, best_keys),
            event_id=event.event_id,
            score=best_score,
            method=event.method,
            url=event.url,
            url_template=_url_template(event.url),
            query_params=event.query_params,
            request_headers=event.request_headers,
            request_body_json=event.request_body_json,
            request_cookie_names=event.request_cookie_names,
            array_path=best_path,
            matched_keys=sorted(best_keys),
            pagination_hints=pagination_hints,
            date_params=date_params,
            card_params=card_params,
            sample_response_shape=_top_level_shape_keys(event.response_json),
            header_templates=_pick_header_templates(event.request_headers),
        )
        candidates.append(candidate)

    dedup: dict[tuple[str, str], EndpointCandidate] = {}
    for candidate in sorted(candidates, key=lambda item: (item.score, item.event_id), reverse=True):
        key = (candidate.method.upper(), candidate.url_template)
        if key not in dedup:
            dedup[key] = candidate

    return list(dedup.values())


def _infer_transactions_candidates(events: list[CapturedEvent]) -> list[EndpointCandidate]:
    reference_event: CapturedEvent | None = None
    for event in reversed(events):
        parsed = urlsplit(event.url)
        host = (parsed.hostname or "").lower()
        if host == "api.cal-online.co.il" and event.method.upper() in {"POST", "GET"}:
            reference_event = event
            break

    if reference_event is None:
        return []

    headers = dict(reference_event.request_headers)
    templates = _pick_header_templates(headers)
    if "x-site-id" in headers:
        templates["x-site-id"] = headers["x-site-id"]

    inferred: list[EndpointCandidate] = []
    for endpoint_url in KNOWN_TX_ENDPOINTS:
        inferred.append(
            EndpointCandidate(
                kind="transactions",
                endpoint_type="history",
                event_id=reference_event.event_id,
                score=1,
                method="POST",
                url=endpoint_url,
                url_template=endpoint_url,
                query_params={},
                request_headers=headers,
                request_body_json={
                    "cardId": "{card_id}",
                    "fromDate": "{from_date}",
                    "toDate": "{to_date}",
                },
                request_cookie_names=list(reference_event.request_cookie_names),
                array_path="$.result.transactions",
                matched_keys=["inferred_known_cal_endpoint"],
                pagination_hints=[],
                date_params=["fromDate", "toDate"],
                card_params=["cardId"],
                sample_response_shape=[],
                header_templates=templates,
                replay={
                    "inferred": True,
                    "reason": "No transactions endpoint captured from UI navigation",
                },
            )
        )

    return inferred


def _strip_url_query(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _build_session_from_storage(storage_state_path: Path) -> tuple[requests.Session, dict[str, Any]]:
    state = json.loads(storage_state_path.read_text(encoding="utf-8"))
    session = requests.Session()

    for cookie in state.get("cookies", []):
        session.cookies.set(
            cookie.get("name", ""),
            cookie.get("value", ""),
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )

    return session, state


def _is_replay_header(name: str) -> bool:
    lower = name.lower()
    if lower in {"cookie", "host", "content-length", "connection"}:
        return False
    if lower.startswith("sec-"):
        return False
    if lower.startswith(":"):
        return False
    return True


def _build_replay_headers(sample_headers: dict[str, str]) -> dict[str, str]:
    return {name: value for name, value in sample_headers.items() if _is_replay_header(name)}


def _is_from_date_key(key: str) -> bool:
    normalized = _normalize_key(key)
    if "from" in normalized and "date" in normalized:
        return True
    return any(hint in normalized for hint in FROM_DATE_HINTS if "date" in normalized or hint in normalized)


def _is_to_date_key(key: str) -> bool:
    normalized = _normalize_key(key)
    if "to" in normalized and "date" in normalized:
        return True
    return any(hint in normalized for hint in TO_DATE_HINTS if "date" in normalized or hint in normalized)


def _is_card_key(key: str) -> bool:
    normalized = _normalize_key(key)
    if "card" in normalized:
        return True
    return any(hint in normalized for hint in CARD_ID_HINTS)


def _format_date_like_sample(sample_value: Any, date_value: date) -> str:
    if isinstance(sample_value, str):
        if "T" in sample_value and sample_value.endswith("Z"):
            return f"{date_value.isoformat()}T00:00:00.000Z"
        if "T" in sample_value and ("+" in sample_value[10:] or "-" in sample_value[10:]):
            return f"{date_value.isoformat()}T00:00:00+00:00"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", sample_value):
            return date_value.isoformat()
    return date_value.isoformat()


def _normalized_date_targets(candidate: EndpointCandidate) -> set[str]:
    targets = {_normalize_key(_leaf_key(name)) for name in candidate.date_params}
    return {target for target in targets if target}


def _override_history_dates_in_json(
    value: Any,
    *,
    normalized_date_targets: set[str],
    from_date: date,
    to_date: date,
) -> Any:
    if isinstance(value, dict):
        overridden: dict[str, Any] = {}
        for key, sub_value in value.items():
            normalized_key = _normalize_key(key)
            if normalized_key in normalized_date_targets and _is_from_date_key(key):
                overridden[key] = _format_date_like_sample(sub_value, from_date)
            elif normalized_key in normalized_date_targets and _is_to_date_key(key):
                overridden[key] = _format_date_like_sample(sub_value, to_date)
            elif normalized_key in normalized_date_targets and _is_date_field_name(key):
                overridden[key] = _format_date_like_sample(sub_value, to_date)
            else:
                overridden[key] = _override_history_dates_in_json(
                    sub_value,
                    normalized_date_targets=normalized_date_targets,
                    from_date=from_date,
                    to_date=to_date,
                )
        return overridden

    if isinstance(value, list):
        return [
            _override_history_dates_in_json(
                item,
                normalized_date_targets=normalized_date_targets,
                from_date=from_date,
                to_date=to_date,
            )
            for item in value
        ]

    return value


def _prepare_candidate_request(
    candidate: EndpointCandidate,
    *,
    from_date: date,
    to_date: date,
) -> tuple[str, dict[str, str], Any | None]:
    params = _filter_volatile_params(candidate.query_params)
    body = copy.deepcopy(candidate.request_body_json)

    updated_params = dict(params)
    if candidate.endpoint_type == "history":
        for key, value in params.items():
            if not _is_date_field_name(key):
                continue
            if _is_from_date_key(key):
                updated_params[key] = _format_date_like_sample(value, from_date)
            elif _is_to_date_key(key):
                updated_params[key] = _format_date_like_sample(value, to_date)
            else:
                updated_params[key] = _format_date_like_sample(value, to_date)

        if body is not None:
            body = _override_history_dates_in_json(
                body,
                normalized_date_targets=_normalized_date_targets(candidate),
                from_date=from_date,
                to_date=to_date,
            )

    return _strip_url_query(candidate.url), updated_params, body


def _do_replay_request(
    session: requests.Session,
    candidate: EndpointCandidate,
    headers: dict[str, str],
    timeout: int = 25,
) -> tuple[int | None, Any | None, str | None, dict[str, str], Any | None]:
    now = date.today()
    from_date = now - timedelta(days=7)
    to_date = now

    url, params, body = _prepare_candidate_request(
        candidate,
        from_date=from_date,
        to_date=to_date,
    )

    kwargs: dict[str, Any] = {
        "headers": headers,
        "params": params,
        "timeout": timeout,
    }

    method = candidate.method.upper()
    if method in {"POST", "PUT", "PATCH", "DELETE"} and body is not None:
        kwargs["json"] = body

    try:
        response = session.request(method, url, **kwargs)
    except requests.RequestException as exc:
        return None, None, f"requests error: {exc}", params, body

    body_json = _safe_parse_json(response.text)
    return response.status_code, body_json, None, params, body


def _authish_header(name: str) -> bool:
    lower = name.lower()
    return any(marker in lower for marker in ("auth", "token", "csrf", "session", "x-"))


def _minimize_required_headers(
    session: requests.Session,
    candidate: EndpointCandidate,
    headers: dict[str, str],
) -> tuple[list[str], dict[str, Any], Any | None]:
    status, body_json, error, used_params, used_body = _do_replay_request(session, candidate, headers)
    replay: dict[str, Any] = {
        "replay_status": status,
        "success": bool(status is not None and status < 400),
        "replay_error": error,
        "replay_used_query_params": used_params,
        "replay_used_request_json": _sanitize_json(used_body),
        "suspected_missing_headers": [],
    }

    if status is None:
        return sorted(headers.keys()), replay, body_json

    if status >= 400:
        replay["suspected_missing_headers"] = sorted(
            name for name in candidate.request_headers if _authish_header(name) and name not in headers
        )
        return sorted(headers.keys()), replay, body_json

    required = dict(headers)
    for name in list(required.keys()):
        trial_headers = {key: value for key, value in required.items() if key != name}
        trial_status, _, trial_error, _, _ = _do_replay_request(
            session,
            candidate,
            trial_headers,
        )

        if trial_status is not None and trial_status < 400 and trial_error is None:
            required = trial_headers

    replay["minimal_status"] = status
    replay["used_headers"] = sorted(headers.keys())
    replay["required_headers_after_minimize"] = sorted(required.keys())
    return sorted(required.keys()), replay, body_json


def _extract_first_card_id(payload: Any) -> str | None:
    if isinstance(payload, list):
        for item in payload:
            found = _extract_first_card_id(item)
            if found:
                return found

    if isinstance(payload, dict):
        keys = list(payload.keys())
        for key in keys:
            normalized = _normalize_key(key)
            if any(hint in normalized for hint in CARD_ID_HINTS):
                value = payload.get(key)
                if isinstance(value, (str, int)) and str(value).strip():
                    return str(value)

        for value in payload.values():
            found = _extract_first_card_id(value)
            if found:
                return found

    return None


def _verify_candidates(
    cards_candidates: list[EndpointCandidate],
    tx_candidates: list[EndpointCandidate],
    storage_state_path: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    replay_summary: dict[str, Any] = {
        "init": None,
        "history": None,
        "dashboard": None,
        "cards": None,
        "transactions": None,
        "errors": [],
    }

    if not storage_state_path.exists():
        replay_summary["errors"].append("storage_state.json is missing, replay skipped")
        return replay_summary

    try:
        session, _state = _build_session_from_storage(storage_state_path)
    except Exception as exc:
        replay_summary["errors"].append(f"Failed to load storage state: {exc}")
        return replay_summary

    try:
        all_candidates = cards_candidates + tx_candidates

        def _pick(preferred_type: str, preferred_path: str | None = None) -> EndpointCandidate | None:
            typed = [item for item in all_candidates if item.endpoint_type == preferred_type]
            if preferred_path:
                for item in typed:
                    if preferred_path in urlsplit(item.url).path.lower():
                        return item
            if typed:
                return sorted(typed, key=lambda item: (item.score, item.event_id), reverse=True)[0]
            if preferred_path:
                path_matched = [
                    item for item in all_candidates if preferred_path in urlsplit(item.url).path.lower()
                ]
                if path_matched:
                    return sorted(path_matched, key=lambda item: (item.score, item.event_id), reverse=True)[0]
            return None

        init_candidate = _pick("init", INIT_ENDPOINT_PATH)
        history_candidate = _pick("history", HISTORY_ENDPOINT_PATH)
        dashboard_candidate = _pick("dashboard", DASHBOARD_ENDPOINT_PATH)

        def _verify_one(candidate: EndpointCandidate | None, key: str) -> dict[str, Any] | None:
            if candidate is None:
                return None
            replay_headers = _build_replay_headers(candidate.request_headers)
            required_headers, replay, _body_json = _minimize_required_headers(
                session,
                candidate,
                replay_headers,
            )
            candidate.required_headers = required_headers
            candidate.replay = replay
            return {
                "status": replay.get("replay_status"),
                "success": replay.get("success"),
                "required_headers": required_headers,
                "suspected_missing_headers": replay.get("suspected_missing_headers", []),
                "replay_error": replay.get("replay_error"),
                "replay_used_request_json": replay.get("replay_used_request_json"),
                "replay_used_query_params": replay.get("replay_used_query_params"),
                "endpoint_type": candidate.endpoint_type,
                "url_template": candidate.url_template,
                "kind": key,
            }

        replay_summary["init"] = _verify_one(init_candidate, "init")
        replay_summary["history"] = _verify_one(history_candidate, "history")
        replay_summary["dashboard"] = _verify_one(dashboard_candidate, "dashboard")
        replay_summary["cards"] = replay_summary["init"]
        replay_summary["transactions"] = replay_summary["history"]

        if not cards_candidates:
            logger.warning("Cards endpoint candidates were not found")
        if not tx_candidates:
            logger.warning("Transactions endpoint candidates were not found")

        return replay_summary
    finally:
        session.close()


def _curl_sample(candidate: EndpointCandidate) -> str:
    parts: list[str] = [f"curl -X {candidate.method.upper()} '{candidate.url_template}'"]

    for header_name in candidate.required_headers:
        value = candidate.header_templates.get(header_name, "<set-via-env-or-session>")
        if _is_sensitive_header(header_name):
            value = "<redacted>"
        parts.append(f"  -H '{header_name}: {value}'")

    if candidate.request_cookie_names:
        cookie_preview = "; ".join(f"{name}=<redacted>" for name in candidate.request_cookie_names)
        parts.append(f"  --cookie '{cookie_preview}'")

    return " \\\n".join(parts)


def _build_markdown_report(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# CAL Digital API Discovery Report")
    lines.append("")
    lines.append(f"- Generated at: `{report['generated_at']}`")
    lines.append(f"- Base URL: `{report['base_url']}`")
    lines.append(f"- Network log: `{report['files']['network_log']}`")
    lines.append("")

    login = report.get("login", {})
    lines.append("## Login")
    lines.append(f"- Status: `{login.get('status', 'unknown')}`")
    if login.get("error"):
        lines.append(f"- Error: `{login['error']}`")
    lines.append("")

    lines.append("## Endpoint Discovery")
    for kind in ("cards", "transactions"):
        discovered = report.get("discovered", {}).get(kind, [])
        lines.append(f"### {kind.capitalize()} candidates ({len(discovered)})")
        if not discovered:
            lines.append("- Not found")
            lines.append("")
            continue

        for index, item in enumerate(discovered, start=1):
            lines.append(f"- Candidate #{index}: `{item['method']} {item['url_template']}`")
            lines.append(f"  - score: `{item['score']}`")
            lines.append(f"  - endpoint_type: `{item.get('endpoint_type', 'unknown')}`")
            lines.append(f"  - required_headers: `{', '.join(item.get('required_headers', [])) or '-'}'")
            lines.append(f"  - required_cookies: `{', '.join(item.get('required_cookies', [])) or '-'}'")
            lines.append(f"  - date params: `{', '.join(item.get('date_params', [])) or '-'}'")
            lines.append(f"  - card params: `{', '.join(item.get('card_params', [])) or '-'}'")
            lines.append(f"  - pagination: `{', '.join(item.get('pagination_hints', [])) or '-'}'")
        lines.append("")

    selected = report.get("selected", {})
    lines.append("## Selected Endpoints")
    for kind in ("cards", "transactions", "init", "history", "dashboard"):
        item = selected.get(kind)
        if not item:
            lines.append(f"- {kind}: not selected")
            continue
        lines.append(f"- {kind}: `{item['method']} {item['url_template']}`")
    lines.append("")

    lines.append("## Replay Verification")
    replay = report.get("replay", {})
    for kind in ("init", "history", "dashboard"):
        payload = replay.get(kind)
        if not payload:
            lines.append(f"- {kind}: skipped")
            continue
        lines.append(
            f"- {kind}: status={payload.get('status')} success={payload.get('success')} required_headers={payload.get('required_headers', [])}"
        )
        if payload.get("suspected_missing_headers"):
            lines.append(f"  - suspected_missing_headers: {payload['suspected_missing_headers']}")
        if payload.get("replay_error"):
            lines.append(f"  - replay_error: {payload['replay_error']}")
    if replay.get("errors"):
        lines.append(f"- replay errors: {replay['errors']}")
    lines.append("")

    lines.append("## cURL Samples")
    for kind in ("cards", "transactions"):
        item = selected.get(kind)
        if not item:
            continue
        lines.append(f"### {kind}")
        lines.append("```bash")
        lines.append(item.get("curl_sample", "# unavailable"))
        lines.append("```")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _build_requests_samples(report: dict[str, Any]) -> str:
    return textwrap.dedent(
        """
        from __future__ import annotations

        import json
        import os
        import re
        from datetime import date, timedelta
        from pathlib import Path
        from typing import Any
        from urllib.parse import urlsplit, urlunsplit

        import requests


        ROOT = Path(__file__).resolve().parent
        REPORT_PATH = ROOT / "api_report.json"
        STORAGE_PATH = ROOT / "storage_state.json"


        def load_report(path: Path = REPORT_PATH) -> dict[str, Any]:
            return json.loads(path.read_text(encoding="utf-8"))


        def load_storage_state(path: Path = STORAGE_PATH) -> dict[str, Any]:
            return json.loads(path.read_text(encoding="utf-8"))


        def create_session(storage_state_path: Path = STORAGE_PATH) -> requests.Session:
            state = load_storage_state(storage_state_path)
            session = requests.Session()
            for cookie in state.get("cookies", []):
                session.cookies.set(
                    cookie.get("name", ""),
                    cookie.get("value", ""),
                    domain=cookie.get("domain"),
                    path=cookie.get("path", "/"),
                )
            return session


        def _normalize_key(name: str) -> str:
            return re.sub(r"[^a-z0-9]", "", name.lower())


        def _strip_url_query(url: str) -> str:
            parsed = urlsplit(url)
            return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


        def _find_storage_token(storage_state: dict[str, Any], hints: list[str]) -> str | None:
            for origin in storage_state.get("origins", []):
                for item in origin.get("localStorage", []):
                    key = str(item.get("name", "")).lower()
                    if any(hint in key for hint in hints):
                        value = item.get("value")
                        if isinstance(value, str) and value:
                            return value
            return None


        def _build_headers(endpoint: dict[str, Any], storage_state: dict[str, Any]) -> dict[str, str]:
            headers = dict(endpoint.get("header_templates", {}))
            required = endpoint.get("required_headers", [])

            csrf_token = _find_storage_token(storage_state, ["csrf", "xsrf"])
            generic_token = _find_storage_token(storage_state, ["token", "auth"])

            for header_name in required:
                if header_name in headers:
                    continue

                lower = header_name.lower()
                env_key = "CAL_HEADER_" + re.sub(r"[^A-Z0-9]+", "_", header_name.upper()).strip("_")
                env_value = os.getenv(env_key)
                if env_value:
                    headers[header_name] = env_value
                    continue

                if "csrf" in lower and csrf_token:
                    headers[header_name] = csrf_token
                    continue

                if "auth" in lower and generic_token:
                    headers[header_name] = generic_token

            return headers


        def _is_card_key(key: str) -> bool:
            normalized = _normalize_key(key)
            return "card" in normalized or normalized in {"id", "cardid", "cardnumber", "pan"}


        def _is_from_date_key(key: str) -> bool:
            normalized = _normalize_key(key)
            return "from" in normalized and "date" in normalized


        def _is_to_date_key(key: str) -> bool:
            normalized = _normalize_key(key)
            return "to" in normalized and "date" in normalized


        def _override_json(value: Any, card_id: str | None, from_date: str, to_date: str) -> Any:
            if isinstance(value, dict):
                out: dict[str, Any] = {}
                for key, sub_value in value.items():
                    if _is_from_date_key(key):
                        out[key] = from_date
                    elif _is_to_date_key(key):
                        out[key] = to_date
                    elif card_id and _is_card_key(key):
                        out[key] = card_id
                    else:
                        out[key] = _override_json(sub_value, card_id, from_date, to_date)
                return out

            if isinstance(value, list):
                return [_override_json(item, card_id, from_date, to_date) for item in value]

            return value


        def _extract_first_card_id(payload: Any) -> str | None:
            if isinstance(payload, list):
                for item in payload:
                    found = _extract_first_card_id(item)
                    if found:
                        return found

            if isinstance(payload, dict):
                for key, value in payload.items():
                    normalized = _normalize_key(key)
                    if "card" in normalized or normalized in {"id", "cardid", "cardnumber", "pan"}:
                        if isinstance(value, (str, int)) and str(value).strip():
                            return str(value)
                for value in payload.values():
                    found = _extract_first_card_id(value)
                    if found:
                        return found

            return None


        def _request_endpoint(
            session: requests.Session,
            endpoint: dict[str, Any],
            storage_state: dict[str, Any],
            *,
            card_id: str | None,
        ) -> requests.Response:
            params = dict(endpoint.get("query_params", {}))
            body = endpoint.get("sample_request_post_data")
            if body is not None:
                body = _override_json(body, card_id, (date.today() - timedelta(days=7)).isoformat(), date.today().isoformat())

            from_date = (date.today() - timedelta(days=7)).isoformat()
            to_date = date.today().isoformat()

            for key in list(params):
                if _is_from_date_key(key):
                    params[key] = from_date
                elif _is_to_date_key(key):
                    params[key] = to_date
                elif card_id and _is_card_key(key):
                    params[key] = card_id

            if endpoint.get("kind") == "transactions" and card_id and not any(_is_card_key(key) for key in params):
                params["cardId"] = card_id
            if endpoint.get("kind") == "transactions" and not any(_is_from_date_key(key) for key in params):
                params.setdefault("fromDate", from_date)
            if endpoint.get("kind") == "transactions" and not any(_is_to_date_key(key) for key in params):
                params.setdefault("toDate", to_date)

            headers = _build_headers(endpoint, storage_state)
            method = endpoint.get("method", "GET").upper()
            url = _strip_url_query(endpoint["url"])

            kwargs: dict[str, Any] = {"headers": headers, "params": params, "timeout": 25}
            if method in {"POST", "PUT", "PATCH", "DELETE"} and body is not None:
                kwargs["json"] = body

            return session.request(method, url, **kwargs)


        def fetch_cards(session: requests.Session | None = None) -> Any:
            report = load_report()
            storage_state = load_storage_state()
            endpoint = report.get("selected", {}).get("cards")
            if not endpoint:
                raise RuntimeError("cards endpoint not found in api_report.json")

            own_session = session is None
            if session is None:
                session = create_session()

            response = _request_endpoint(session, endpoint, storage_state, card_id=None)
            if own_session:
                session.close()
            response.raise_for_status()
            return response.json()


        def fetch_transactions(card_id: str | None, from_date: str | None = None, to_date: str | None = None, session: requests.Session | None = None) -> Any:
            report = load_report()
            storage_state = load_storage_state()
            endpoint = report.get("selected", {}).get("transactions")
            if not endpoint:
                raise RuntimeError("transactions endpoint not found in api_report.json")

            own_session = session is None
            if session is None:
                session = create_session()

            if card_id is None:
                cards_payload = fetch_cards(session=session)
                card_id = _extract_first_card_id(cards_payload)

            response = _request_endpoint(session, endpoint, storage_state, card_id=card_id)
            if own_session:
                session.close()
            response.raise_for_status()
            return response.json()


        if __name__ == "__main__":
            session = create_session()
            try:
                cards = fetch_cards(session=session)
                print(json.dumps(cards, ensure_ascii=False, indent=2)[:2000])
            finally:
                session.close()
        """
    ).strip() + "\n"


class NetworkCollector:
    def __init__(self, output_path: Path, logger: logging.Logger) -> None:
        self.output_path = output_path
        self.logger = logger
        self.events: list[CapturedEvent] = []
        self._event_counter = 0

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text("", encoding="utf-8")

    def on_response(self, response: Any) -> None:
        try:
            request = response.request
            if request.resource_type not in {"xhr", "fetch"}:
                return

            self._event_counter += 1
            event_id = self._event_counter

            request_headers = dict(request.headers)
            request_cookie_names = _cookie_names_from_header(request_headers.get("cookie"))
            query_params = _query_params_from_url(request.url)

            request_post_data = request.post_data
            request_post_json = _safe_parse_json(request_post_data)
            request_json_sanitized: Any | None = None
            request_post_preview: dict[str, Any] | None = None
            if request_post_json is not None:
                request_json_sanitized = _sanitize_json(request_post_json)
                serialized = json.dumps(request_json_sanitized, ensure_ascii=False)
                preview_text, preview_truncated = _truncate_text_bytes(serialized, MAX_JSON_PREVIEW_BYTES)
                request_post_preview = {
                    "json_preview": preview_text,
                    "truncated": preview_truncated,
                }
            elif request_post_data:
                request_post_preview = {
                    "kind": "non_json",
                    "size_bytes": len(request_post_data.encode("utf-8", errors="ignore")),
                }

            response_headers = dict(response.headers)
            status = int(response.status)

            response_json = None
            response_payload: dict[str, Any] = {
                "status": status,
                "headers": _mask_headers(response_headers),
                "content_type": response_headers.get("content-type"),
            }

            body_text: str | None
            try:
                body_text = response.text()
            except Exception:
                body_text = None

            parsed_response_json = _safe_parse_json(body_text)
            if parsed_response_json is not None:
                response_json = parsed_response_json
                sanitized_response = _sanitize_json(parsed_response_json)
                serialized = json.dumps(sanitized_response, ensure_ascii=False)
                preview_text, preview_truncated = _truncate_text_bytes(serialized, MAX_JSON_PREVIEW_BYTES)
                response_payload["response_json_preview"] = preview_text
                response_payload["response_json_truncated"] = preview_truncated
            else:
                if body_text is not None:
                    response_payload["body_size_bytes"] = len(body_text.encode("utf-8", errors="ignore"))

            log_record = {
                "event_id": event_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "request": {
                    "resource_type": request.resource_type,
                    "method": request.method,
                    "url": request.url,
                    "query_params": query_params,
                    "headers": _mask_headers(request_headers),
                    "request_json": request_json_sanitized,
                    "post_data": request_post_preview,
                },
                "response": response_payload,
            }

            with self.output_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(log_record, ensure_ascii=False) + "\n")

            self.events.append(
                CapturedEvent(
                    event_id=event_id,
                    method=request.method,
                    url=request.url,
                    query_params=query_params,
                    request_headers=request_headers,
                    request_body_json=request_post_json,
                    request_cookie_names=request_cookie_names,
                    response_status=status,
                    response_headers=response_headers,
                    response_json=response_json,
                )
            )
        except Exception:
            self.logger.exception("Failed to capture network event")


def _targets(page: Any) -> list[Any]:
    targets = [page]
    for frame in page.frames:
        if frame != page.main_frame:
            targets.append(frame)
    return targets


def _has_non_empty_value(field: Any) -> bool:
    try:
        return bool(field.input_value().strip())
    except Exception:
        return False


def _fill_and_verify(field: Any, value: str) -> bool:
    try:
        field.fill(value, timeout=1500)
        if _has_non_empty_value(field):
            return True
    except Exception:
        pass

    try:
        field.click(timeout=1000)
        field.press("ControlOrMeta+a", timeout=1000)
        field.type(value, delay=40, timeout=3000)
        if _has_non_empty_value(field):
            return True
    except Exception:
        pass

    return False


def _try_fill_first(target: Any, selectors: Iterable[str], value: str) -> bool:
    for selector in selectors:
        locator = target.locator(selector)
        try:
            count = locator.count()
        except Exception:
            continue

        for index in range(min(count, 10)):
            field = locator.nth(index)
            if _fill_and_verify(field, value):
                return True
    return False


def _is_allowed_page_url(url: str) -> bool:
    host = urlsplit(url).hostname or ""
    return any(host == allowed or host.endswith(f".{allowed}") for allowed in ALLOWED_PAGE_HOSTS)


def _close_page_if_disallowed(
    page: Any,
    logger: logging.Logger,
    *,
    popup_only: bool = False,
) -> None:
    try:
        url = page.url or ""
    except Exception:
        return

    if not url or url in {"about:blank", "about:srcdoc"}:
        return
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        return

    if popup_only:
        try:
            if page.opener() is None:
                return
        except Exception:
            return

    if _is_allowed_page_url(url):
        return

    logger.warning("Closing external page opened during discovery: %s", url)
    try:
        page.close()
    except Exception:
        logger.exception("Failed to close external page: %s", url)


def _click_first_clickable(
    locator: Any,
    *,
    forbidden_markers: tuple[str, ...] = (),
) -> bool:
    try:
        count = locator.count()
    except Exception:
        return False

    for idx in range(min(count, 10)):
        candidate = locator.nth(idx)
        try:
            text = (candidate.inner_text(timeout=400) or "").strip().lower()
        except Exception:
            text = ""
        if text and any(marker in text for marker in forbidden_markers):
            continue

        try:
            candidate.click(timeout=1500)
            return True
        except Exception:
            continue
    return False


def _switch_via_js(target: Any, needle: str) -> bool:
    script = """
    (needle) => {
      const selectors = ["button", "a", "[role='button']", "[role='tab']", "label", "div", "span"];
      const nodes = Array.from(document.querySelectorAll(selectors.join(",")));
      for (const el of nodes) {
        const text = (el.textContent || "").trim();
        if (!text.includes(needle)) continue;
        el.click();
        return true;
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
    ({ value, isPassword }) => {
      const lower = (s) => (s || "").toLowerCase();
      const has = (s, parts) => parts.some((p) => lower(s).includes(p));
      const visible = (el) => !!(el.offsetParent || el.getClientRects().length);
      const inputs = Array.from(document.querySelectorAll("input"))
        .filter((el) => !el.disabled && !el.readOnly && visible(el));

      const userWords = ["user", "login", "id", "userid", "name", "שם", "תעודת", "זהות", "tz"];
      const passWords = ["pass", "password", "pwd", "סיסמ", "קוד"];
      const candidates = inputs.filter((el) => {
        const type = lower(el.getAttribute("type"));
        if (isPassword) {
          return type === "password" || has(el.name, passWords) || has(el.id, passWords) || has(el.placeholder, passWords) || has(el.getAttribute("aria-label"), passWords);
        }
        return type !== "password";
      });
      if (!candidates.length) return false;

      let best = null;
      let bestScore = -1;
      for (const el of candidates) {
        let score = 0;
        const type = lower(el.getAttribute("type"));
        const key = `${el.name || ""} ${el.id || ""} ${el.placeholder || ""} ${el.getAttribute("aria-label") || ""}`;
        if (isPassword) {
          if (type === "password") score += 5;
          if (has(key, passWords)) score += 4;
        } else {
          if (type === "text" || type === "email" || type === "tel" || type === "number" || !type) score += 2;
          if (has(key, userWords)) score += 5;
        }
        if (score > bestScore) {
          bestScore = score;
          best = el;
        }
      }
      if (!best) best = candidates[0];

      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
      if (setter) setter.call(best, value);
      else best.value = value;
      best.dispatchEvent(new Event("input", { bubbles: true }));
      best.dispatchEvent(new Event("change", { bubbles: true }));
      best.dispatchEvent(new Event("blur", { bubbles: true }));
      return !!String(best.value || "").trim();
    }
    """
    try:
        return bool(target.evaluate(script, {"value": value, "isPassword": is_password}))
    except Exception:
        return False


def _switch_to_username_login(page: Any) -> bool:
    switch_text = "כניסה עם שם משתמש"
    switch_short = "שם משתמש"
    selector_templates = [
        "button:has-text('{text}')",
        "a:has-text('{text}')",
        "[role='button']:has-text('{text}')",
        "[role='tab']:has-text('{text}')",
        "[aria-label*='{text}']",
        "[title*='{text}']",
        "text={text}",
    ]

    for _ in range(8):
        for target in _targets(page):
            locators = [
                target.get_by_role("tab", name=switch_text, exact=False),
                target.get_by_role("button", name=switch_text, exact=False),
                target.get_by_role("link", name=switch_text, exact=False),
                target.get_by_text(switch_text, exact=False),
                target.get_by_text(switch_short, exact=False),
            ]
            for text in (switch_text, switch_short):
                for template in selector_templates:
                    locators.append(target.locator(template.format(text=text)))

            for locator in locators:
                if _click_first_clickable(locator):
                    page.wait_for_timeout(900)
                    return True

            if _switch_via_js(target, switch_short):
                page.wait_for_timeout(900)
                return True

        page.wait_for_timeout(800)
    return False


def _prefill_credentials(page: Any, username: str, password: str) -> tuple[bool, bool]:
    username_selectors = (
        "input[autocomplete='username']",
        "input[name='username']",
        "input[name='userName']",
        "input[name='userCode']",
        "input[name='userid']",
        "input[name='id']",
        "input[name*='user' i]",
        "input[name*='login' i]",
        "input[name*='id' i]",
        "input[id*='user' i]",
        "input[id*='login' i]",
        "input[id*='id' i]",
        "input[aria-label*='שם משתמש']",
        "input[placeholder*='שם משתמש']",
        "input[aria-label*='תעודת']",
        "input[placeholder*='תעודת']",
        "input[type='tel']",
        "input[type='number']",
        "input[type='email']",
        "input[type='text']",
    )
    password_selectors = (
        "input[autocomplete='current-password']",
        "input[name='password']",
        "input[id*='pass' i]",
        "input[type='password']",
    )

    username_filled = False
    password_filled = False
    for target in _targets(page):
        if not username_filled:
            username_filled = _try_fill_first(target, username_selectors, username)
            if not username_filled:
                username_filled = _fill_via_js_fallback(target, username, is_password=False)
        if not password_filled:
            password_filled = _try_fill_first(target, password_selectors, password)
            if not password_filled:
                password_filled = _fill_via_js_fallback(target, password, is_password=True)
        if username_filled and password_filled:
            break

    return username_filled, password_filled


def _collect_input_hints(page: Any) -> list[str]:
    script = """
    () => {
      const visible = (el) => !!(el.offsetParent || el.getClientRects().length);
      return Array.from(document.querySelectorAll("input"))
        .filter((el) => visible(el))
        .slice(0, 20)
        .map((el) => {
          const type = el.getAttribute("type") || "";
          const name = el.getAttribute("name") || "";
          const id = el.getAttribute("id") || "";
          const placeholder = el.getAttribute("placeholder") || "";
          const aria = el.getAttribute("aria-label") || "";
          return `${type}|${name}|${id}|${placeholder}|${aria}`;
        });
    }
    """
    hints: list[str] = []
    for target in _targets(page):
        try:
            values = target.evaluate(script)
            if isinstance(values, list):
                hints.extend(str(item) for item in values if isinstance(item, str))
        except Exception:
            continue
    return hints[:30]


def _click_by_keywords(
    page: Any,
    keywords: Iterable[str],
    *,
    forbidden_markers: tuple[str, ...] = FORBIDDEN_NAV_MARKERS,
) -> bool:
    for target in _targets(page):
        for keyword in keywords:
            selectors = [
                f"button:has-text('{keyword}')",
                f"a:has-text('{keyword}')",
                f"[role='button']:has-text('{keyword}')",
                f"[role='tab']:has-text('{keyword}')",
                f"text={keyword}",
            ]
            for selector in selectors:
                locator = target.locator(selector)
                if _click_first_clickable(locator, forbidden_markers=forbidden_markers):
                    return True
    return False


def _submit_with_enter_on_password(page: Any) -> bool:
    for target in _targets(page):
        locator = target.locator("input[type='password']")
        try:
            count = locator.count()
        except Exception:
            continue
        for idx in range(min(count, 5)):
            field = locator.nth(idx)
            try:
                field.focus(timeout=1200)
                field.press("Enter", timeout=1200)
                return True
            except Exception:
                continue
    return False


def _login(page: Any, username: str, password: str, logger: logging.Logger) -> None:
    switched = _switch_to_username_login(page)
    if switched:
        logger.info("Switched login form to username/password mode")
    else:
        logger.info("Username login switch was not detected, continuing with direct fill")

    filled_user = False
    filled_pass = False
    for _ in range(3):
        filled_user, filled_pass = _prefill_credentials(page, username, password)
        if filled_user and filled_pass:
            break
        page.wait_for_timeout(900)

    if not filled_user or not filled_pass:
        input_hints = _collect_input_hints(page)
        raise RuntimeError(
            "Could not fill login form fields (username/password). "
            f"Visible inputs sample: {input_hints}"
        )

    login_keywords = (
        "Login",
        "Sign in",
        "Submit",
        "Continue",
        "כניסה",
        "התחבר",
        "התחברות",
        "אישור",
    )
    clicked = _submit_with_enter_on_password(page)
    if clicked:
        logger.info("Submitted login by pressing Enter in password field")
    else:
        clicked = _click_by_keywords(
            page,
            login_keywords,
            forbidden_markers=FORBIDDEN_NAV_MARKERS + FORBIDDEN_LOGIN_MARKERS,
        )

    if not clicked:
        for target in _targets(page):
            locator = target.locator("input[type='password']")
            try:
                if locator.count() > 0:
                    locator.first.press("Enter", timeout=1500)
                    clicked = True
                    break
            except Exception:
                continue

    if not clicked:
        raise RuntimeError("Could not submit login form")

    logger.info("Login form submitted, waiting for authenticated UI")
    page.wait_for_timeout(8000)


def _ensure_authenticated(page: Any, base_url: str, logger: logging.Logger) -> None:
    urls: list[str] = []
    try:
        urls.append(page.url)
    except Exception:
        pass

    for frame in page.frames:
        try:
            if frame.url:
                urls.append(frame.url)
        except Exception:
            continue

    lowered = [url.lower() for url in urls if url]
    if any("business.cal-online.co.il" in url for url in lowered):
        raise RuntimeError(
            "Navigation switched to business.cal-online.co.il (business login flow), "
            "not the personal CAL Digital flow."
        )

    otp_markers = ("send-otp", "/otp", "one-time", "verification-code")
    if any(any(marker in url for marker in otp_markers) for url in lowered):
        raise RuntimeError(
            "OTP step detected after login submit. This account currently requires OTP, "
            "so automated discovery cannot continue in password-only mode."
        )

    if any("/login" in url or "/calconnect/" in url for url in lowered):
        dashboard_url = f"{base_url.rstrip('/')}/dashboard"
        logger.info("Still on login/calconnect page, trying direct dashboard URL")
        try:
            page.goto(dashboard_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3500)
        except Exception:
            logger.exception("Failed to open dashboard URL after login submit")

    final_url = (page.url or "").lower()
    if any(marker in final_url for marker in otp_markers):
        raise RuntimeError(
            "OTP step detected after dashboard redirect attempt."
        )
    if "/login" in final_url or "/calconnect/" in final_url:
        raise RuntimeError(
            "Login was not completed: browser is still on login/calconnect flow."
        )


def _navigate_ui(page: Any, logger: logging.Logger) -> tuple[bool, bool]:
    cards_keywords = (
        "Cards",
        "כרטיסים",
        "הכרטיסים",
        "כרטיסי אשראי",
    )
    tx_keywords = (
        "Transactions",
        "Activity",
        "Operations",
        "עסקאות",
        "פעולות",
        "חיובים",
    )

    cards_clicked = _click_by_keywords(page, cards_keywords)
    if cards_clicked:
        logger.info("Navigated to cards section")
        page.wait_for_timeout(4000)
    else:
        logger.warning("Cards section click not confirmed")

    tx_clicked = _click_by_keywords(page, tx_keywords)
    if tx_clicked:
        logger.info("Navigated to transactions section")
        page.wait_for_timeout(5000)
    else:
        logger.warning("Transactions section click not confirmed")

    page.wait_for_timeout(3000)
    return cards_clicked, tx_clicked


def _warmup_transaction_routes(page: Any, base_url: str, logger: logging.Logger) -> None:
    for path in TX_WARMUP_PATHS:
        if page.is_closed():
            logger.warning("Skipping remaining warmup routes: page is already closed")
            return
        url = f"{base_url.rstrip('/')}{path}"
        try:
            logger.info("Trying transaction warmup route: %s", url)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)
        except Exception:
            logger.exception("Transaction warmup route failed: %s", url)


def _validate_env() -> tuple[str, str]:
    dotenv_values: dict[str, str] = {}
    for dotenv_path in _dotenv_candidates(os.getenv("CAL_DOTENV_PATH", DEFAULT_DOTENV_PATH)):
        if dotenv_path.exists():
            dotenv_values = _load_dotenv(dotenv_path)
            break

    username_names = (PRIMARY_USERNAME_ENV, *FALLBACK_USERNAME_ENVS)
    password_names = (PRIMARY_PASSWORD_ENV, *FALLBACK_PASSWORD_ENVS)
    username = _read_credential(username_names, dotenv_values)
    password = _read_credential(password_names, dotenv_values)

    if not username or not password:
        raise RuntimeError(
            "CAL credentials are required. Set CAL_USERNAME/CAL_PASSWORD in environment "
            "or .env file."
        )

    return username, password


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _pick_candidate(
    candidates: list[EndpointCandidate],
    *,
    endpoint_type: str | None = None,
    path_contains: str | None = None,
) -> EndpointCandidate | None:
    pool = candidates
    if endpoint_type:
        typed = [item for item in pool if item.endpoint_type == endpoint_type]
        if typed:
            pool = typed

    if path_contains:
        path_matched = [item for item in pool if path_contains in urlsplit(item.url).path.lower()]
        if path_matched:
            pool = path_matched

    if not pool:
        return None
    return sorted(pool, key=lambda item: (item.score, item.event_id), reverse=True)[0]


def _runtime_headers(candidate: EndpointCandidate | None) -> dict[str, str]:
    if candidate is None:
        return {}
    allowed = {
        "authorization",
        "x-site-id",
        "user-agent",
        "referer",
        "accept",
        "content-type",
        "origin",
        "x-requested-with",
    }
    out: dict[str, str] = {}
    for name, value in candidate.request_headers.items():
        if name.lower() in allowed and isinstance(value, str) and value:
            out[name] = value
    return out


def _build_runtime_session_payload(
    *,
    base_url: str,
    cards_candidates: list[EndpointCandidate],
    tx_candidates: list[EndpointCandidate],
) -> dict[str, Any]:
    all_candidates = cards_candidates + tx_candidates
    init = _pick_candidate(all_candidates, endpoint_type="init", path_contains=INIT_ENDPOINT_PATH)
    history = _pick_candidate(all_candidates, endpoint_type="history", path_contains=HISTORY_ENDPOINT_PATH)
    dashboard = _pick_candidate(all_candidates, endpoint_type="dashboard", path_contains=DASHBOARD_ENDPOINT_PATH)

    def _entry(candidate: EndpointCandidate | None) -> dict[str, Any] | None:
        if candidate is None:
            return None
        return {
            "method": candidate.method.upper(),
            "url": candidate.url,
            "url_template": candidate.url_template,
            "headers": _runtime_headers(candidate),
            "request_json": candidate.request_body_json,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "endpoints": {
            "init": _entry(init),
            "history": _entry(history),
            "dashboard": _entry(dashboard),
        },
    }


def _build_report(
    *,
    base_url: str,
    login_status: str,
    login_error: str | None,
    events: list[CapturedEvent],
    cards_candidates: list[EndpointCandidate],
    tx_candidates: list[EndpointCandidate],
    replay: dict[str, Any],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    all_candidates = cards_candidates + tx_candidates
    selected_init = _pick_candidate(
        all_candidates,
        endpoint_type="init",
        path_contains=INIT_ENDPOINT_PATH,
    )
    selected_history = _pick_candidate(
        all_candidates,
        endpoint_type="history",
        path_contains=HISTORY_ENDPOINT_PATH,
    )
    selected_dashboard = _pick_candidate(
        all_candidates,
        endpoint_type="dashboard",
        path_contains=DASHBOARD_ENDPOINT_PATH,
    )

    selected_cards = selected_init or _pick_candidate(cards_candidates)
    selected_tx = selected_history or _pick_candidate(tx_candidates)

    def _selected_payload(candidate: EndpointCandidate | None) -> dict[str, Any] | None:
        if candidate is None:
            return None
        payload = _candidate_to_public(candidate)
        payload["curl_sample"] = _curl_sample(candidate)
        return payload

    selected_cards_public = _selected_payload(selected_cards)
    selected_tx_public = _selected_payload(selected_tx)

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "login": {
            "status": login_status,
            "error": login_error,
        },
        "network": {
            "events_total": len(events),
            "xhr_fetch_events": len(events),
        },
        "discovered": {
            "cards": [_candidate_to_public(candidate) for candidate in cards_candidates],
            "transactions": [_candidate_to_public(candidate) for candidate in tx_candidates],
        },
        "selected": {
            "cards": selected_cards_public,
            "transactions": selected_tx_public,
            "init": _selected_payload(selected_init),
            "history": _selected_payload(selected_history),
            "dashboard": _selected_payload(selected_dashboard),
        },
        "replay": replay,
        "diagnostics": diagnostics,
        "files": {
            "network_log": str(NETWORK_LOG_PATH),
            "storage_state": str(STORAGE_STATE_PATH),
            "runtime_session": str(RUNTIME_SESSION_PATH),
            "api_report_json": str(REPORT_JSON_PATH),
            "api_report_md": str(REPORT_MD_PATH),
            "requests_samples": str(REQUESTS_SAMPLES_PATH),
            "diagnostics": str(DIAGNOSTICS_PATH),
        },
    }

    return report


def _attach_context_guards(context: Any, logger: logging.Logger) -> None:
    def _on_new_page(new_page: Any) -> None:
        def _on_dom_ready() -> None:
            _close_page_if_disallowed(new_page, logger, popup_only=True)

        def _on_frame_navigated(frame: Any) -> None:
            try:
                if frame == new_page.main_frame:
                    _close_page_if_disallowed(new_page, logger, popup_only=True)
            except Exception:
                return

        try:
            new_page.on("domcontentloaded", lambda: _on_dom_ready())
            new_page.on("framenavigated", _on_frame_navigated)
        except Exception:
            logger.exception("Failed to attach domcontentloaded guard for popup page")

    context.on("page", _on_new_page)


def run_discovery(base_url: str, debug: bool) -> int:
    logger = _configure_logging(debug)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    diagnostics: dict[str, Any] = {
        "debug": debug,
        "base_url": base_url,
        "steps": [],
        "error": None,
        "traceback": None,
    }

    login_status = "not_started"
    login_error: str | None = None
    events: list[CapturedEvent] = []
    cards_candidates: list[EndpointCandidate] = []
    tx_candidates: list[EndpointCandidate] = []
    replay_summary: dict[str, Any] = {"cards": None, "transactions": None, "errors": []}

    try:
        username, password = _validate_env()
    except Exception as exc:
        diagnostics["error"] = str(exc)
        diagnostics["traceback"] = traceback.format_exc()
        _write_json(DIAGNOSTICS_PATH, diagnostics)
        print(f"[error] {exc}")
        return 2

    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:
        message = (
            "playwright is not installed. Install with `pip install playwright` "
            "and run `playwright install chromium`."
        )
        diagnostics["error"] = message
        diagnostics["traceback"] = str(exc)
        _write_json(DIAGNOSTICS_PATH, diagnostics)
        print(f"[error] {message}")
        return 2

    collector = NetworkCollector(NETWORK_LOG_PATH, logger)

    with sync_playwright() as playwright:
        browser = None
        context = None
        page = None
        try:
            launch_kwargs: dict[str, Any] = {
                "headless": not debug,
            }
            if debug:
                launch_kwargs["slow_mo"] = 250

            diagnostics["steps"].append("launch_browser")
            browser = playwright.chromium.launch(**launch_kwargs)
            context = browser.new_context(ignore_https_errors=True)
            context.on("response", collector.on_response)
            _attach_context_guards(context, logger)
            page = context.new_page()

            diagnostics["steps"].append("open_login_page")
            page.goto(base_url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(4000)
            _close_page_if_disallowed(page, logger)

            diagnostics["steps"].append("login")
            _login(page, username, password, logger)
            login_status = "submitted"

            diagnostics["steps"].append("ensure_authenticated")
            _ensure_authenticated(page, base_url, logger)

            diagnostics["steps"].append("navigate_ui")
            _cards_clicked, tx_clicked = _navigate_ui(page, logger)

            if not tx_clicked:
                diagnostics["steps"].append("warmup_transaction_routes")
                _warmup_transaction_routes(page, base_url, logger)

            diagnostics["steps"].append("wait_network_settle")
            if page.is_closed():
                raise RuntimeError(
                    "Discovery page was closed before network settle. "
                    "Likely due navigation outside the expected login flow."
                )
            page.wait_for_timeout(5000)

            diagnostics["steps"].append("save_storage_state")
            context.storage_state(path=str(STORAGE_STATE_PATH))

            events = collector.events
            cards_candidates = _discover_candidates(events, kind="cards")
            tx_candidates = _discover_candidates(events, kind="transactions")
            if not tx_candidates:
                inferred_tx = _infer_transactions_candidates(events)
                if inferred_tx:
                    tx_candidates = inferred_tx
                    logger.warning(
                        "Transactions endpoint candidates inferred from known CAL patterns: %s",
                        [item.url_template for item in inferred_tx],
                    )

            diagnostics["steps"].append("replay_verification")
            replay_summary = _verify_candidates(
                cards_candidates,
                tx_candidates,
                STORAGE_STATE_PATH,
                logger,
            )

            login_status = "ok"
        except Exception as exc:
            login_status = "failed"
            login_error = str(exc)
            diagnostics["error"] = str(exc)
            diagnostics["traceback"] = traceback.format_exc()
            logger.exception("Discovery run failed")
            events = collector.events
            if not cards_candidates and events:
                cards_candidates = _discover_candidates(events, kind="cards")
            if not tx_candidates and events:
                tx_candidates = _discover_candidates(events, kind="transactions")
            if not tx_candidates and events:
                tx_candidates = _infer_transactions_candidates(events)

            if page is not None:
                screenshot_path = OUTPUT_ROOT / "failure_screenshot.png"
                try:
                    if not page.is_closed():
                        page.screenshot(path=str(screenshot_path), full_page=True)
                        diagnostics["failure_screenshot"] = str(screenshot_path)
                except Exception:
                    logger.exception("Failed to save screenshot")
        finally:
            if context is not None:
                try:
                    if not STORAGE_STATE_PATH.exists():
                        context.storage_state(path=str(STORAGE_STATE_PATH))
                except Exception:
                    logger.exception("Failed to save storage state in finally")
                context.close()
            if browser is not None:
                browser.close()

    report = _build_report(
        base_url=base_url,
        login_status=login_status,
        login_error=login_error,
        events=events,
        cards_candidates=cards_candidates,
        tx_candidates=tx_candidates,
        replay=replay_summary,
        diagnostics=diagnostics,
    )
    runtime_payload = _build_runtime_session_payload(
        base_url=base_url,
        cards_candidates=cards_candidates,
        tx_candidates=tx_candidates,
    )

    _write_json(REPORT_JSON_PATH, report)
    _write_json(RUNTIME_SESSION_PATH, runtime_payload)
    _write_json(DIAGNOSTICS_PATH, diagnostics)
    REPORT_MD_PATH.write_text(_build_markdown_report(report), encoding="utf-8")
    REQUESTS_SAMPLES_PATH.write_text(_build_requests_samples(report), encoding="utf-8")

    print(f"[ok] network log: {NETWORK_LOG_PATH}")
    print(f"[ok] report json: {REPORT_JSON_PATH}")
    print(f"[ok] report md: {REPORT_MD_PATH}")
    print(f"[ok] requests samples: {REQUESTS_SAMPLES_PATH}")
    print(f"[ok] storage state: {STORAGE_STATE_PATH}")
    print(f"[ok] runtime session: {RUNTIME_SESSION_PATH}")

    cards_found = len(cards_candidates)
    tx_found = len(tx_candidates)
    print(f"[summary] cards candidates: {cards_found}, transactions candidates: {tx_found}")

    if login_status != "ok":
        print("[warn] discovery finished with errors; see diagnostics.json")
        return 1

    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover CAL Digital private API via Playwright")
    parser.add_argument(
        "--base-url",
        default=os.getenv("CAL_BASE_URL", DEFAULT_BASE_URL),
        help="CAL Digital entrypoint URL",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    debug = os.getenv("CAL_DEBUG", "0") == "1"
    return run_discovery(base_url=args.base_url, debug=debug)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
