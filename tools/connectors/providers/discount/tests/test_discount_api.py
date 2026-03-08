from __future__ import annotations

from datetime import date

import pytest

from tools.connectors._core.errors import InvalidCredentialsError
from tools.connectors._core.types import SyncWindow
from tools.connectors.providers.discount import api
from tools.connectors.providers.discount.fetch import DiscountHints


class DummySession:
    def __init__(self, name: str):
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _hints() -> DiscountHints:
    return DiscountHints(
        by_date_url_template="https://start.telebank.co.il/Titan/gatewayAPI/lastTransactions/transactions/{account_number}/ByDate",
        keepalive_url="https://start.telebank.co.il/Titan/gatewayAPI/getCurrentTimestamp",
        account_number="0072141958",
        required_headers=("accept",),
        runtime_headers={},
        observed_headers={"language": "HEBREW", "site": "retail"},
    )


def test_sync_relogin_after_invalid_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    s1 = DummySession("first")
    s2 = DummySession("second")
    hints = _hints()

    calls = {"ensure": 0, "fetch": 0}

    def fake_ensure_session(state):
        calls["ensure"] += 1
        if calls["ensure"] == 1:
            return s1, hints, {"storage_state_path": "a"}
        return s2, hints, {"storage_state_path": "b"}

    def fake_retry(operation, **kwargs):
        return operation()

    def fake_keepalive(session, _hints_obj):
        return None

    def fake_fetch(session, _hints_obj, *, from_date, to_date):
        calls["fetch"] += 1
        if session is s1:
            raise InvalidCredentialsError("expired")
        return {"CurrentAccountLastTransactions": {"OperationEntry": []}}

    monkeypatch.setattr(api, "ensure_session", fake_ensure_session)
    monkeypatch.setattr(api, "retry_call", fake_retry)
    monkeypatch.setattr(api, "ping_keepalive", fake_keepalive)
    monkeypatch.setattr(api, "fetch_by_date", fake_fetch)

    out = api.sync(
        SyncWindow(from_date=date(2026, 3, 1), to_date=date(2026, 3, 8)),
        state={},
    )

    assert calls["ensure"] == 2
    assert calls["fetch"] == 2
    assert out["_state_update"]["storage_state_path"] == "b"
    assert out["_state_update"]["last_sync"] == "2026-03-08"
    assert s1.closed is True
    assert s2.closed is True
