from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests

from tools.connectors._core.errors import InvalidCredentialsError, RateLimitError, TemporaryError


LOG = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 30
DEFAULT_BASE_URL = "https://start.telebank.co.il"
DEFAULT_BYDATE_TEMPLATE = (
    "https://start.telebank.co.il/Titan/gatewayAPI/lastTransactions/transactions/{account_number}/ByDate"
)
DEFAULT_KEEPALIVE_URL = "https://start.telebank.co.il/Titan/gatewayAPI/getCurrentTimestamp"
DEFAULT_REFERER = "https://start.telebank.co.il/apollo/retail2/"
DEFAULT_USER_AGENT = "findom-discount/1.0 (+requests)"

REPO_ROOT = Path(__file__).resolve().parents[4]
DISCOVERY_ROOT = REPO_ROOT / ".state" / "discount" / "api_discovery"
STORAGE_STATE_PATH = DISCOVERY_ROOT / "storage_state.json"
SAVED_REQUEST_NAME = "last_transactions_by_date.template.json"


@dataclass(slots=True, frozen=True)
class DiscountHints:
    by_date_url_template: str
    keepalive_url: str
    account_number: str
    required_headers: tuple[str, ...]
    runtime_headers: dict[str, str]
    observed_headers: dict[str, str]


def _json_load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_research_runs() -> list[Path]:
    root = REPO_ROOT / ".state" / "discount" / "research"
    if not root.exists():
        return []
    return sorted([item for item in root.iterdir() if item.is_dir()], reverse=True)


def _load_saved_request_template() -> dict[str, Any]:
    for run_dir in _iter_research_runs():
        candidate = run_dir / "saved_requests" / SAVED_REQUEST_NAME
        if candidate.exists():
            try:
                return _json_load(candidate)
            except Exception:
                continue
    return {}


def _extract_account_from_url(url: str) -> str:
    parts = urlsplit(url).path.strip("/").split("/")
    if "transactions" not in parts:
        return ""
    idx = parts.index("transactions")
    if idx + 1 >= len(parts):
        return ""
    return parts[idx + 1]


def _load_latest_runtime_request() -> dict[str, Any]:
    marker = "/Titan/gatewayAPI/lastTransactions/transactions/"
    for run_dir in _iter_research_runs():
        req_path = run_dir / "network_requests.jsonl"
        if not req_path.exists():
            continue
        try:
            lines = req_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue

        for line in reversed(lines):
            try:
                event = json.loads(line)
            except Exception:
                continue
            if str(event.get("method", "")).upper() != "GET":
                continue
            url = str(event.get("url", ""))
            if marker in url and "/ByDate" in url:
                return event
    return {}


def _load_cookies(path: Path) -> requests.cookies.RequestsCookieJar:
    if not path.exists():
        raise TemporaryError(f"Discount storage state not found: {path}")

    payload = _json_load(path)
    jar = requests.cookies.RequestsCookieJar()
    for item in payload.get("cookies", []):
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


def _parse_action_required(payload: Any) -> bool:
    if isinstance(payload, dict):
        if "actionRequired" in payload:
            return True
        return any(_parse_action_required(value) for value in payload.values())
    if isinstance(payload, list):
        return any(_parse_action_required(item) for item in payload)
    return False


def _response_json(response: requests.Response) -> Any:
    status = response.status_code
    if status in (401, 403):
        raise InvalidCredentialsError(f"Discount auth failed with status={status}")
    if status == 429:
        raise RateLimitError("Discount rate limit reached")
    if 500 <= status < 600:
        raise TemporaryError(f"Discount temporary server error status={status}")
    if status >= 400:
        raise TemporaryError(f"Discount unexpected response status={status}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise TemporaryError("Discount returned non-JSON response") from exc

    if _parse_action_required(payload):
        raise InvalidCredentialsError("Discount session expired (actionRequired)")

    return payload


def _headers_for_request(hints: DiscountHints, account_number: str) -> dict[str, str]:
    headers: dict[str, str] = {
        "accept": "application/json, text/plain, */*",
        "user-agent": DEFAULT_USER_AGENT,
        "referer": DEFAULT_REFERER,
        "language": hints.observed_headers.get("language", "HEBREW"),
        "site": hints.observed_headers.get("site", "retail"),
        "accountnumber": account_number,
    }
    headers.update(hints.observed_headers)
    headers.update(hints.runtime_headers)
    headers["accountnumber"] = account_number
    return {k: str(v) for k, v in headers.items() if v not in (None, "")}


def load_api_hints() -> DiscountHints:
    template = _load_saved_request_template()
    runtime = _load_latest_runtime_request()

    template_url = str(template.get("url_template") or DEFAULT_BYDATE_TEMPLATE)
    runtime_url = str(runtime.get("url", ""))
    if runtime_url and "/ByDate" in runtime_url:
        by_date_url_template = runtime_url.split("?", 1)[0]
    else:
        by_date_url_template = template_url

    account_number = ""
    if runtime_url:
        account_number = _extract_account_from_url(runtime_url)

    query_template = template.get("query_template", {})
    if not isinstance(query_template, dict):
        query_template = {}

    required_headers = tuple(str(x).lower() for x in template.get("required_headers", []))
    observed_headers = {
        str(k).lower(): str(v)
        for k, v in template.get("observed_header_values", {}).items()
        if isinstance(k, str) and isinstance(v, str)
    }
    runtime_headers = {
        str(k).lower(): str(v)
        for k, v in runtime.get("headers", {}).items()
        if isinstance(k, str) and isinstance(v, str)
    }

    if "language" not in observed_headers and "language" in runtime_headers:
        observed_headers["language"] = runtime_headers["language"]
    if "site" not in observed_headers and "site" in runtime_headers:
        observed_headers["site"] = runtime_headers["site"]

    if not account_number:
        account_number = str(runtime_headers.get("accountnumber", "")).strip()

    if not account_number:
        raise TemporaryError(
            "Discount account number was not found in research artifacts. "
            "Run tools/research/run_explorer.py --target discount first."
        )

    if not by_date_url_template:
        by_date_url_template = DEFAULT_BYDATE_TEMPLATE

    if "{account_number}" not in by_date_url_template:
        by_date_url_template = DEFAULT_BYDATE_TEMPLATE

    keepalive_url = DEFAULT_KEEPALIVE_URL

    if query_template:
        # We keep query defaults in runtime headers and params builder.
        # This branch exists to acknowledge template presence for diagnostics.
        pass

    return DiscountHints(
        by_date_url_template=by_date_url_template,
        keepalive_url=keepalive_url,
        account_number=account_number,
        required_headers=required_headers,
        runtime_headers=runtime_headers,
        observed_headers=observed_headers,
    )


def build_session(state: dict[str, Any], hints: DiscountHints) -> requests.Session:
    path_value = state.get("storage_state_path") or str(STORAGE_STATE_PATH)
    storage_path = Path(path_value)

    session = requests.Session()
    session.cookies = _load_cookies(storage_path)
    session.headers.update(_headers_for_request(hints, hints.account_number))
    return session


def ping_keepalive(session: requests.Session, hints: DiscountHints) -> None:
    try:
        response = session.get(hints.keepalive_url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        raise TemporaryError("Discount keepalive failed") from exc
    _response_json(response)


def fetch_by_date(
    session: requests.Session,
    hints: DiscountHints,
    *,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    account_number = hints.account_number
    url = hints.by_date_url_template.format(account_number=account_number)

    params = {
        "FromDate": from_date.strftime("%Y%m%d"),
        "ToDate": to_date.strftime("%Y%m%d"),
        "IsTransactionDetails": "True",
        "IsFutureTransactionFlag": "True",
        "IsEventNames": "True",
        "IsCategoryDescCode": "True",
    }

    try:
        response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        raise TemporaryError("Discount transactions request failed") from exc

    payload = _response_json(response)
    if not isinstance(payload, dict):
        raise TemporaryError("Discount transactions response has invalid format")

    return payload


def validate_session(session: requests.Session, hints: DiscountHints) -> None:
    today = date.today()
    fetch_by_date(session, hints, from_date=today, to_date=today)
