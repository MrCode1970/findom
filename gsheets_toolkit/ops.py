from __future__ import annotations

from gsheets_toolkit.client import SheetsClient
from gsheets_toolkit.models import SheetSchema
from gsheets_toolkit.schema import headers, validate_sheet_schema
from gsheets_toolkit.utils import index_to_column_letter


def _find_sheet_props(client: SheetsClient, title: str) -> dict | None:
    spreadsheet = client.get_spreadsheet()
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == title:
            return props
    return None


def ensure_sheet(
    client: SheetsClient,
    sheet_title: str,
    min_rows: int = 1000,
    min_cols: int = 26,
) -> int:
    props = _find_sheet_props(client, sheet_title)
    if props is None:
        client.batch_update_requests(
            [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_title,
                            "gridProperties": {
                                "rowCount": max(1, min_rows),
                                "columnCount": max(1, min_cols),
                            },
                        }
                    }
                }
            ]
        )
        props = _find_sheet_props(client, sheet_title)
        if props is None:
            raise RuntimeError(f"Sheet {sheet_title} was not created")

    sheet_id = int(props["sheetId"])
    grid = props.get("gridProperties", {})
    row_count = int(grid.get("rowCount", 0))
    col_count = int(grid.get("columnCount", 0))
    if row_count < min_rows or col_count < min_cols:
        client.batch_update_requests(
            [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": max(row_count, min_rows),
                                "columnCount": max(col_count, min_cols),
                            },
                        },
                        "fields": "gridProperties.rowCount,gridProperties.columnCount",
                    }
                }
            ]
        )
    return sheet_id


def apply_sheet_schema(client: SheetsClient, sheet_schema: SheetSchema) -> None:
    validate_sheet_schema(sheet_schema)
    sheet_id = ensure_sheet(
        client,
        sheet_schema.title,
        min_rows=1000,
        min_cols=max(26, len(sheet_schema.columns)),
    )
    column_count = len(sheet_schema.columns)
    last_col_letter = index_to_column_letter(column_count - 1)

    client.batch_update_values(
        [
            {
                "range": f"{sheet_schema.title}!A1:{last_col_letter}1",
                "values": [headers(sheet_schema)],
            }
        ]
    )

    requests: list[dict] = []
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": max(0, sheet_schema.freeze_rows),
                    },
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    if sheet_schema.filter:
        requests.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": column_count,
                        }
                    }
                }
            }
        )
    else:
        requests.append({"clearBasicFilter": {"sheetId": sheet_id}})

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": column_count,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
                    }
                },
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        }
    )

    for idx, col in enumerate(sheet_schema.columns):
        dim_properties: dict = {}
        if col.width is not None:
            dim_properties["pixelSize"] = int(col.width)
        if col.hidden:
            dim_properties["hiddenByUser"] = True
        elif col.width is not None:
            dim_properties["hiddenByUser"] = False

        if not dim_properties:
            continue

        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": idx,
                        "endIndex": idx + 1,
                    },
                    "properties": dim_properties,
                    "fields": ",".join(dim_properties.keys()),
                }
            }
        )

    client.batch_update_requests(requests)

