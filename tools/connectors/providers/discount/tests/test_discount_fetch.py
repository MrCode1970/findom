from __future__ import annotations

from datetime import date

import pytest

from tools.connectors._core.errors import InvalidCredentialsError
from tools.connectors.providers.discount.fetch import DiscountHints, fetch_by_date, validate_session


class DummyResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if not self._responses:
            raise AssertionError("No response prepared")
        return self._responses.pop(0)


def _hints() -> DiscountHints:
    return DiscountHints(
        by_date_url_template="https://start.telebank.co.il/Titan/gatewayAPI/lastTransactions/transactions/{account_number}/ByDate",
        keepalive_url="https://start.telebank.co.il/Titan/gatewayAPI/getCurrentTimestamp",
        account_number="0072141958",
        required_headers=("accept",),
        runtime_headers={},
        observed_headers={"language": "HEBREW", "site": "retail"},
    )


def test_fetch_by_date_success() -> None:
    session = DummySession(
        [DummyResponse(200, {"CurrentAccountLastTransactions": {"OperationEntry": []}})]
    )
    payload = fetch_by_date(session, _hints(), from_date=date(2026, 3, 1), to_date=date(2026, 3, 8))
    assert "CurrentAccountLastTransactions" in payload
    assert session.calls[0]["params"]["FromDate"] == "20260301"
    assert session.calls[0]["params"]["ToDate"] == "20260308"


def test_fetch_by_date_detects_expired_session() -> None:
    session = DummySession([DummyResponse(200, {"actionRequired": {"serviceName": "login"}})])
    with pytest.raises(InvalidCredentialsError):
        fetch_by_date(session, _hints(), from_date=date(2026, 3, 8), to_date=date(2026, 3, 8))


def test_validate_session_uses_by_date() -> None:
    session = DummySession(
        [DummyResponse(200, {"CurrentAccountLastTransactions": {"OperationEntry": []}})]
    )
    validate_session(session, _hints())
    assert len(session.calls) == 1
