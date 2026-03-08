from __future__ import annotations

from tools.connectors.providers.cal_digital.api import (
    _group_transactions_by_card,
    _merge_history_transactions,
)


def test_merge_history_deduplicates_by_stable_transaction_id() -> None:
    clearance = [
        {
            "trnIntId": "txn-1",
            "cardUniqueID": "card-1",
            "trnPurchaseDate": "2026-03-01T10:00:00",
            "trnAmt": "100.00",
        },
        {
            "trnIntId": "txn-2",
            "cardUniqueID": "card-1",
            "trnPurchaseDate": "2026-03-02T10:00:00",
            "trnAmt": "50.00",
        },
    ]
    filtered = [
        {
            "trnIntId": "txn-1",
            "cardUniqueID": "card-1",
            "trnPurchaseDate": "2026-03-01T10:00:00",
            "trnAmt": "100.00",
        },
        {
            "trnIntId": "txn-3",
            "cardUniqueID": "card-1",
            "trnPurchaseDate": "2026-03-03T10:00:00",
            "trnAmt": "20.00",
        },
    ]

    merged, stats = _merge_history_transactions(
        card_id="card-1",
        clearance_items=clearance,
        filtered_items=filtered,
    )

    assert stats == {
        "clearance_raw": 2,
        "filtered_raw": 2,
        "merged_unique": 3,
    }
    assert [item["trnIntId"] for item in merged] == ["txn-1", "txn-2", "txn-3"]
    assert merged[0]["source_endpoint"] == "clearance"
    assert merged[2]["source_endpoint"] == "filtered"


def test_merge_history_uses_fallback_key_when_transaction_id_missing() -> None:
    clearance = [
        {
            "cardUniqueID": "card-2",
            "trnPurchaseDate": "2026-03-01T10:00:00",
            "amount": "12.34",
            "merchantName": "Shop",
        }
    ]
    filtered = [
        {
            "cardUniqueID": "card-2",
            "trnPurchaseDate": "2026-03-01T10:00:00",
            "amount": "12.34",
            "merchantName": "Shop",
        }
    ]

    merged, stats = _merge_history_transactions(
        card_id="card-2",
        clearance_items=clearance,
        filtered_items=filtered,
    )

    assert stats["merged_unique"] == 1
    assert len(merged) == 1
    assert merged[0]["source_endpoint"] == "clearance"


def test_group_transactions_by_card_uses_transaction_card_id() -> None:
    transactions = [
        {"cardUniqueID": "card-a", "trnAmt": 1},
        {"cardUniqueId": "card-b", "trnAmt": 2},
        {"trnAmt": 3},
    ]

    grouped = _group_transactions_by_card(transactions)

    assert list(grouped.keys()) == ["card-a", "card-b", "unknown"]
    assert len(grouped["card-a"]) == 1
    assert len(grouped["card-b"]) == 1
    assert len(grouped["unknown"]) == 1
