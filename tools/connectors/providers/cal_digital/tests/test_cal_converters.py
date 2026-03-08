from __future__ import annotations

import json
from pathlib import Path

from tools.connectors.providers.cal_digital.converters import convert, convert_with_diagnostics


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def _load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _build_raw_bundle() -> dict:
    return {
        "cards": _load_fixture("cards.json"),
        "txns_by_card": {
            "123456": _load_fixture("transactions_card1.json"),
        },
    }


def test_signed_amount_expense_is_negative() -> None:
    result = convert(_build_raw_bundle())
    expense = next(m for m in result.movements if m.external_id == "cal:123456:txn-001")
    assert str(expense.amount) == "-120.50"


def test_external_id_is_stable() -> None:
    first = convert(_build_raw_bundle())
    second = convert(_build_raw_bundle())

    first_ids = [movement.external_id for movement in first.movements]
    second_ids = [movement.external_id for movement in second.movements]
    assert first_ids == second_ids


def test_date_is_iso_datetime() -> None:
    result = convert(_build_raw_bundle())
    expense = next(m for m in result.movements if m.external_id == "cal:123456:txn-001")
    assert expense.occurred_at.isoformat() == "2026-02-20T08:15:00+00:00"


def test_accounts_have_cal_prefix() -> None:
    result = convert(_build_raw_bundle())
    assert result.accounts
    assert all(account.account_id.startswith("cal:") for account in result.accounts)


def test_converter_handles_missing_merchant_comment() -> None:
    result = convert(_build_raw_bundle())
    no_meta_movement = next(m for m in result.movements if m.external_id != "cal:123456:txn-001")
    assert no_meta_movement.merchant is None
    assert no_meta_movement.comment is None


def test_cal_fields_are_mapped_for_refund_transaction() -> None:
    raw = {
        "cards": {"cards": [{"cardUniqueId": "1809016822025293", "cardType": "Demo"}]},
        "txns_by_card": {
            "1809016822025293": {
                "result": {
                    "transArr": [
                        {
                            "trnIntId": "30347390426",
                            "cardUniqueId": "1809016822025293",
                            "merchantName": "PP*2197CODE",
                            "trnPurchaseDate": "2026-02-02T00:00:00",
                            "trnAmt": 8.0,
                            "trnType": "זיכוי",
                            "refundInd": True,
                        }
                    ]
                }
            }
        },
    }
    result = convert(raw)
    assert len(result.movements) == 1
    movement = result.movements[0]
    assert movement.external_id == "cal:1809016822025293:30347390426"
    assert str(movement.amount) == "8.00"
    assert movement.occurred_at.isoformat() == "2026-02-02T00:00:00+00:00"


def test_converter_diagnostics_include_counts() -> None:
    result, diag = convert_with_diagnostics(_build_raw_bundle())
    assert diag["raw_transactions_total"] >= len(result.movements)
    assert diag["converted_movements"] == len(result.movements)
    assert "skipped_in_converter" in diag
