from __future__ import annotations

from tools.connectors.providers.cal_digital.fetch import (
    _hydrate_redacted_body_fields,
    build_history_request_body,
)


def test_history_body_builder_updates_only_date_fields() -> None:
    endpoint = {
        "date_fields": ["fromTransDate", "toTransDate", "nested.createdDate"],
        "sample_request_json": {
            "bankAccountUniqueID": "1172141958003041",
            "cards": [{"cardUniqueID": "111"}, {"cardUniqueID": "222"}],
            "fromTransDate": "2025-02-27T19:17:25.712Z",
            "toTransDate": "2026-02-27T19:17:25.712Z",
            "caller": "dashboard",
            "nested": {
                "createdDate": "2025-01-10",
                "keep": "x",
            },
        },
    }

    built = build_history_request_body(
        endpoint,
        from_date="2026-02-20",
        to_date="2026-02-27",
        init_context=None,
    )
    assert isinstance(built, dict)

    assert set(built.keys()) == set(endpoint["sample_request_json"].keys())
    assert built["bankAccountUniqueID"] == "1172141958003041"
    assert built["cards"] == [{"cardUniqueID": "111"}, {"cardUniqueID": "222"}]
    assert built["caller"] == "dashboard"
    assert built["fromTransDate"] == "2026-02-20T00:00:00.000Z"
    assert built["toTransDate"] == "2026-02-27T00:00:00.000Z"
    assert built["nested"]["createdDate"] == "2026-02-27"
    assert built["nested"]["keep"] == "x"


def test_redacted_tokenguid_is_hydrated_from_storage_tokens() -> None:
    payload = {"tokenGuid": "<redacted>", "other": "ok"}
    storage_tokens = {"tokenGuid": "abc-guid-token"}

    built = _hydrate_redacted_body_fields(payload, storage_tokens)
    assert built["tokenGuid"] == "abc-guid-token"
    assert built["other"] == "ok"


def test_filtered_history_profile_matches_ui_search_pattern() -> None:
    endpoint = {
        "url": "https://api.cal-online.co.il/Transactions/api/filteredTransactions/getFilteredTransactions",
        "date_fields": ["fromTransDate", "toTransDate"],
        "sample_request_json": {
            "bankAccountUniqueID": "old",
            "cards": [{"cardUniqueID": "old-card"}],
            "fromTransDate": "2025-03-06T15:28:17.127Z",
            "toTransDate": "2026-03-06T15:28:17.127Z",
            "merchantHebName": "x",
            "merchantHebCity": "y",
            "trnType": 6,
            "fromTrnAmt": 1,
            "toTrnAmt": 1,
            "transactionsOrigin": 1,
            "transCardPresentInd": 1,
            "walletTranInd": 0,
            "caller": "dashboard",
        },
    }

    built = build_history_request_body(
        endpoint,
        from_date="2026-03-06",
        to_date="2026-03-06",
        init_context={
            "bank_account_unique_id": "1172141958003041",
            "card_unique_ids": ["c1", "c2", "c3"],
        },
    )
    assert isinstance(built, dict)
    assert built["bankAccountUniqueID"] == "1172141958003041"
    assert built["cards"] == [{"cardUniqueID": "c1"}, {"cardUniqueID": "c2"}, {"cardUniqueID": "c3"}]
    assert built["trnType"] == 0
    assert built["caller"] == "module_search"
    assert built["fromTrnAmt"] == 0
    assert built["toTrnAmt"] == 0
    assert built["transactionsOrigin"] == 0
    assert built["transCardPresentInd"] == 0
    assert "walletTranInd" not in built
    assert built["fromTransDate"].endswith("Z")
    assert built["toTransDate"].endswith("Z")
