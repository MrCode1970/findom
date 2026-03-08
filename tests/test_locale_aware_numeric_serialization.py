from __future__ import annotations

from typing import Any

from gsheets_toolkit.upsert import upsert_rows_snapshot


class DummyClient:
    def __init__(self, locale: str) -> None:
        self.locale = locale
        self.batch_calls: list[tuple[list[dict[str, Any]], str]] = []
        self.clears: list[str] = []

    def get_values(self, _a1_range: str) -> list[list[str]]:
        return []

    def get_spreadsheet_locale(self) -> str:
        return self.locale

    def batch_update_values(self, updates: list[dict[str, Any]], value_input_option: str = "RAW") -> None:
        self.batch_calls.append((updates, value_input_option))

    def clear_values(self, a1_range: str) -> None:
        self.clears.append(a1_range)


def _sample_rows() -> tuple[list[list[Any]], list[str]]:
    rows = [
        [
            "2026-03-01T10:00:00+00:00",
            15.8,
            "ILS",
            "Merchant",
            "",
            "card_main",
            "cal_digital",
            "ext_1",
            "2026-03-06T12:00:00+00:00",
        ]
    ]
    return rows, ["ext_1"]


def test_upsert_locale_aware_numeric_ru_uses_comma_and_user_entered() -> None:
    client = DummyClient(locale="ru_RU")
    rows, keys = _sample_rows()

    upsert_rows_snapshot(
        client=client,  # type: ignore[arg-type]
        sheet_title="Journal",
        key_col_letter="H",
        columns_span="A:I",
        start_row=2,
        rows=rows,
        keys=keys,
        numeric_column_indexes=[1],
        locale_aware_numeric=True,
    )

    updates, value_input_option = client.batch_calls[0]
    assert value_input_option == "USER_ENTERED"
    assert updates[0]["values"][0][1] == "15,8"
    assert updates[0]["values"][0][7] == "ext_1"


def test_upsert_locale_aware_numeric_non_ru_keeps_raw_number() -> None:
    client = DummyClient(locale="en_US")
    rows, keys = _sample_rows()

    upsert_rows_snapshot(
        client=client,  # type: ignore[arg-type]
        sheet_title="Journal",
        key_col_letter="H",
        columns_span="A:I",
        start_row=2,
        rows=rows,
        keys=keys,
        numeric_column_indexes=[1],
        locale_aware_numeric=True,
    )

    updates, value_input_option = client.batch_calls[0]
    assert value_input_option == "RAW"
    assert updates[0]["values"][0][1] == 15.8
