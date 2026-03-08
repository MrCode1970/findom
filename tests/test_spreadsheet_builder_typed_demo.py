from __future__ import annotations

from tables.spreadsheet_builder import _demo_rows_request, _demo_user_entered_value


def test_demo_user_entered_value_types() -> None:
    assert _demo_user_entered_value(15.8) == {"userEnteredValue": {"numberValue": 15.8}}
    assert _demo_user_entered_value(True) == {"userEnteredValue": {"boolValue": True}}
    assert _demo_user_entered_value("text") == {"userEnteredValue": {"stringValue": "text"}}
    assert _demo_user_entered_value("=A1+B1") == {"userEnteredValue": {"formulaValue": "=A1+B1"}}


def test_demo_rows_request_builds_update_cells_with_typed_values() -> None:
    sheet = {
        "title": "Normalized",
        "columns": [
            {"key": "a", "header": "a"},
            {"key": "b", "header": "b"},
            {"key": "c", "header": "c"},
            {"key": "d", "header": "d"},
        ],
        "demo_rows": [
            [15.8, True, "=A2*2", "label"],
        ],
    }

    request = _demo_rows_request(123, sheet)
    assert request is not None

    update = request["updateCells"]
    assert update["fields"] == "userEnteredValue"
    assert update["range"]["sheetId"] == 123
    assert update["range"]["startRowIndex"] == 1
    assert update["range"]["endRowIndex"] == 2

    values = update["rows"][0]["values"]
    assert values[0] == {"userEnteredValue": {"numberValue": 15.8}}
    assert values[1] == {"userEnteredValue": {"boolValue": True}}
    assert values[2] == {"userEnteredValue": {"formulaValue": "=A2*2"}}
    assert values[3] == {"userEnteredValue": {"stringValue": "label"}}
