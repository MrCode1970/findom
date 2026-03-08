from __future__ import annotations

import json
from pathlib import Path

from tools.connectors.providers.discount.converters import convert


FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "last_transactions_sample.json"
)


def test_discount_convert_basic() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    raw = {
        "last_transactions": payload,
        "request_meta": {"account_number": "0072141958"},
    }

    result = convert(raw)

    assert len(result.accounts) == 1
    account = result.accounts[0]
    assert account.account_id == "discount:0072141958"
    assert account.currency == "ILS"

    assert len(result.movements) == 2
    ids = {m.external_id for m in result.movements}
    assert "discount:0072141958:URN-3002" in ids
    assert "discount:0072141958:URN-3003" in ids

    amounts = sorted(str(m.amount) for m in result.movements)
    assert amounts == ["-118.55", "13260.00"]
