from __future__ import annotations

import importlib.util
import json
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from gsheets_toolkit.models import ColumnSpec, SheetSchema, SpreadsheetSchema
from gsheets_toolkit.schema import validate_spreadsheet_schema
from gsheets_toolkit.utils import index_to_column_letter, require_env

if TYPE_CHECKING:
    from gsheets_toolkit.client import SheetsClient


def _sheet_id_by_title(client: SheetsClient) -> dict[str, int]:
    spreadsheet = client.get_spreadsheet()
    mapping: dict[str, int] = {}
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        title = str(props.get("title", "")).strip()
        if not title:
            continue
        mapping[title] = int(props["sheetId"])
    return mapping


def _column_count(sheet: dict[str, Any]) -> int:
    columns = sheet.get("columns", [])
    return len(columns) if columns else 1


def _normalize_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for col in columns:
        normalized.append(
            {
                "key": str(col["key"]),
                "header": str(col["header"]),
                "width": col.get("width"),
                "hidden": bool(col.get("hidden", False)),
            }
        )
    return normalized


def _normalize_sheet(sheet: dict[str, Any]) -> dict[str, Any]:
    columns = _normalize_columns(list(sheet.get("columns", [])))
    return {
        "title": str(sheet["title"]),
        "columns": columns,
        "freeze_rows": int(sheet.get("freeze_rows", 1)),
        "filter": bool(sheet.get("filter", True)),
        "min_rows": int(sheet.get("min_rows", 1000)),
        "min_cols": int(sheet.get("min_cols", max(26, len(columns)))),
        "clear": bool(sheet.get("clear", False)),
        "clear_range": sheet.get("clear_range"),
        "formulas": list(sheet.get("formulas", [])),
        "number_formats": list(sheet.get("number_formats", [])),
        "text_formats": list(sheet.get("text_formats", [])),
        "conditional_formats": list(sheet.get("conditional_formats", [])),
        "batch_requests": list(sheet.get("batch_requests", [])),
        "demo_rows": list(sheet.get("demo_rows", [])),
        "snapshot": sheet.get("snapshot"),
    }


def _sheet_schema_from_dict(sheet: dict[str, Any]) -> SheetSchema:
    return SheetSchema(
        title=sheet["title"],
        columns=[
            ColumnSpec(
                key=str(col["key"]),
                header=str(col["header"]),
                width=int(col["width"]) if col.get("width") is not None else None,
                hidden=bool(col.get("hidden", False)),
            )
            for col in sheet["columns"]
        ],
        freeze_rows=int(sheet.get("freeze_rows", 1)),
        filter=bool(sheet.get("filter", True)),
    )


def _default_header_request(sheet_id: int, column_count: int) -> dict[str, Any]:
    return {
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


def _grid_requests(sheet: dict[str, Any], sheet_id: int, column_count: int) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": max(0, int(sheet.get("freeze_rows", 1))),
                    },
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    ]

    if sheet.get("filter", True):
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

    requests.append(_default_header_request(sheet_id, column_count))

    for idx, col in enumerate(sheet["columns"]):
        properties: dict[str, Any] = {}
        if col.get("width") is not None:
            properties["pixelSize"] = int(col["width"])
        if col.get("hidden"):
            properties["hiddenByUser"] = True
        elif col.get("width") is not None:
            properties["hiddenByUser"] = False

        if not properties:
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
                    "properties": properties,
                    "fields": ",".join(properties.keys()),
                }
            }
        )

    return requests


def _split_a1_range(a1: str) -> tuple[str, str]:
    parts = a1.split("!", 1)
    if len(parts) != 2:
        raise ValueError(f"A1 range must include sheet title: {a1}")
    return parts[0], parts[1]


def _a1_to_grid_range(sheet_id: int, a1_range: str) -> dict[str, int]:
    _, cells = _split_a1_range(a1_range)
    start, end = cells.split(":", 1) if ":" in cells else (cells, cells)

    def parse_cell(cell: str) -> tuple[int, int, bool, bool]:
        text = cell.strip().upper()
        letters = ""
        digits = ""
        for ch in text:
            if "A" <= ch <= "Z":
                if digits:
                    raise ValueError(f"Invalid A1 cell: {cell}")
                letters += ch
            elif "0" <= ch <= "9":
                digits += ch
            else:
                raise ValueError(f"Invalid A1 cell: {cell}")
        if not letters:
            raise ValueError(f"Invalid A1 cell: {cell}")
        col = 0
        for ch in letters:
            col = col * 26 + (ord(ch) - ord("A") + 1)
        col -= 1
        row = int(digits) - 1 if digits else 0
        return row, col, bool(digits), bool(letters)

    start_row, start_col, has_start_row, _ = parse_cell(start)
    end_row, end_col, has_end_row, has_end_col = parse_cell(end)

    if end_row < start_row:
        end_row = start_row
    if end_col < start_col:
        end_col = start_col

    result = {
        "sheetId": sheet_id,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col + 1 if has_end_col else start_col + 1,
    }
    if has_start_row:
        result["startRowIndex"] = start_row
    else:
        result["startRowIndex"] = 0
    if has_end_row:
        result["endRowIndex"] = end_row + 1
    return result


def _formula_requests(sheet_id: int, sheet_title: str, formulas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for item in formulas:
        mode = str(item.get("mode", "cell")).strip().lower()
        if mode == "autofill":
            source = f"{sheet_title}!{item['source_range']}"
            destination = f"{sheet_title}!{item['destination_range']}"
            source_grid = _a1_to_grid_range(sheet_id, source)
            destination_grid = _a1_to_grid_range(sheet_id, destination)
            dimension = str(item.get("dimension", "ROWS")).upper()
            if dimension == "COLUMNS":
                src_end = int(source_grid.get("endColumnIndex", source_grid["startColumnIndex"] + 1))
                dst_end = int(destination_grid.get("endColumnIndex", destination_grid["startColumnIndex"] + 1))
            else:
                src_end = int(source_grid.get("endRowIndex", source_grid["startRowIndex"] + 1))
                dst_end = int(destination_grid.get("endRowIndex", destination_grid["startRowIndex"] + 1))
            fill_length = int(item.get("fill_length", max(0, dst_end - src_end)))
            requests.append(
                {
                    "autoFill": {
                        "sourceAndDestination": {
                            "source": source_grid,
                            "dimension": dimension,
                            "fillLength": fill_length,
                        },
                        "useAlternateSeries": bool(item.get("alternate_series", False)),
                    }
                }
            )
            continue

        if mode == "range":
            target = f"{sheet_title}!{item['range']}"
        else:
            target = f"{sheet_title}!{item['cell']}"

        requests.append(
            {
                "repeatCell": {
                    "range": _a1_to_grid_range(sheet_id, target),
                    "cell": {"userEnteredValue": {"formulaValue": str(item["formula"])}},
                    "fields": "userEnteredValue.formulaValue",
                }
            }
        )
    return requests


def _text_format_requests(
    sheet_id: int,
    sheet_title: str,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for item in items:
        a1 = f"{sheet_title}!{item['range']}"
        fmt: dict[str, Any] = {}
        fields: list[str] = []
        if "text_color" in item:
            fmt["textFormat"] = fmt.get("textFormat", {})
            fmt["textFormat"]["foregroundColor"] = item["text_color"]
            fields.append("userEnteredFormat.textFormat.foregroundColor")
        if "background_color" in item:
            fmt["backgroundColor"] = item["background_color"]
            fields.append("userEnteredFormat.backgroundColor")
        if "bold" in item:
            fmt["textFormat"] = fmt.get("textFormat", {})
            fmt["textFormat"]["bold"] = bool(item["bold"])
            fields.append("userEnteredFormat.textFormat.bold")
        if not fields:
            continue

        requests.append(
            {
                "repeatCell": {
                    "range": _a1_to_grid_range(sheet_id, a1),
                    "cell": {"userEnteredFormat": fmt},
                    "fields": ",".join(fields),
                }
            }
        )
    return requests


def _number_format_requests(
    sheet_id: int,
    sheet_title: str,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for item in items:
        a1 = f"{sheet_title}!{item['range']}"
        number_format = {
            "type": str(item.get("type", "NUMBER")).upper(),
            "pattern": str(item.get("pattern", "0.00")),
        }
        requests.append(
            {
                "repeatCell": {
                    "range": _a1_to_grid_range(sheet_id, a1),
                    "cell": {"userEnteredFormat": {"numberFormat": number_format}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )
    return requests


def _conditional_format_requests(
    sheet_id: int,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for item in items:
        rule = dict(item["rule"])
        ranges = rule.get("ranges")
        if isinstance(ranges, list):
            converted: list[dict[str, Any]] = []
            for entry in ranges:
                if "sheetId" in entry:
                    converted.append(entry)
                elif "a1" in entry:
                    converted.append(_a1_to_grid_range(sheet_id, str(entry["a1"])))
                else:
                    converted.append(entry)
            rule["ranges"] = converted

        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": rule,
                    "index": int(item.get("index", 0)),
                }
            }
        )
    return requests


def _header_updates_for_sheet(sheet: dict[str, Any]) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    column_count = _column_count(sheet)
    right_col = index_to_column_letter(column_count - 1)
    headers = [col["header"] for col in sheet["columns"]]

    updates.append(
        {
            "range": f"{sheet['title']}!A1:{right_col}1",
            "values": [headers],
        }
    )
    return updates


def _demo_user_entered_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, bool):
        return {"userEnteredValue": {"boolValue": value}}
    if isinstance(value, (int, float)):
        return {"userEnteredValue": {"numberValue": float(value)}}
    if isinstance(value, str):
        if value.startswith("="):
            return {"userEnteredValue": {"formulaValue": value}}
        return {"userEnteredValue": {"stringValue": value}}
    return {"userEnteredValue": {"stringValue": str(value)}}


def _demo_rows_request(sheet_id: int, sheet: dict[str, Any]) -> dict[str, Any] | None:
    demo_rows = list(sheet.get("demo_rows", []))
    if not demo_rows:
        return None

    column_count = _column_count(sheet)
    row_data: list[dict[str, Any]] = []
    for row in demo_rows:
        raw_values = list(row[:column_count])
        if len(raw_values) < column_count:
            raw_values.extend([None] * (column_count - len(raw_values)))
        row_data.append(
            {
                "values": [_demo_user_entered_value(value) for value in raw_values],
            }
        )

    return {
        "updateCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 1 + len(demo_rows),
                "startColumnIndex": 0,
                "endColumnIndex": column_count,
            },
            "rows": row_data,
            "fields": "userEnteredValue",
        }
    }


def _apply_snapshot_load(client: SheetsClient, sheet: dict[str, Any]) -> int:
    from gsheets_toolkit.upsert import upsert_rows_snapshot

    snapshot = sheet.get("snapshot")
    if not snapshot:
        return 0

    rows = snapshot.get("rows", [])
    keys = snapshot.get("keys", [])

    upsert_rows_snapshot(
        client=client,
        sheet_title=sheet["title"],
        key_col_letter=str(snapshot["key_col_letter"]),
        columns_span=str(snapshot["columns_span"]),
        start_row=int(snapshot.get("start_row", 2)),
        rows=[["" if value is None else value for value in row] for row in rows],
        keys=[str(item) for item in keys],
    )
    return len(rows)


def _maybe_clear_sheet(client: SheetsClient, sheet: dict[str, Any]) -> None:
    clear_range = sheet.get("clear_range")
    if isinstance(clear_range, str) and clear_range.strip():
        client.clear_values(f"{sheet['title']}!{clear_range}")
        return

    if not sheet.get("clear", False):
        return

    column_count = _column_count(sheet)
    right_col = index_to_column_letter(column_count - 1)
    client.clear_values(f"{sheet['title']}!A2:{right_col}")


def _normalize_python_schema(module: Any) -> dict[str, Any]:
    builder_schema = getattr(module, "SPREADSHEET_BUILDER_SCHEMA", None)
    if isinstance(builder_schema, dict):
        normalized = _normalize_json_schema(builder_schema)
        if "spreadsheet_id" not in normalized:
            normalized["spreadsheet_id"] = getattr(module, "SPREADSHEET_ID", None)
        return normalized

    schema_obj: SpreadsheetSchema | None = None
    for attr in (
        "SPREADSHEET_SCHEMA",
        "LOG_SPREADSHEET_SCHEMA",
        "SCHEMA",
    ):
        candidate = getattr(module, attr, None)
        if isinstance(candidate, SpreadsheetSchema):
            schema_obj = candidate
            break

    if schema_obj is None:
        for value in module.__dict__.values():
            if isinstance(value, SpreadsheetSchema):
                schema_obj = value
                break

    if schema_obj is None:
        raise RuntimeError("Python schema does not define SpreadsheetSchema")

    validate_spreadsheet_schema(schema_obj)

    sheets = []
    for sheet in schema_obj.sheets:
        data = asdict(sheet)
        data["columns"] = [asdict(col) for col in sheet.columns]
        sheets.append(_normalize_sheet(data))

    value_updates = list(getattr(module, "VALUE_UPDATES", []))
    spreadsheet_id = getattr(module, "SPREADSHEET_ID", None)

    return {
        "name": getattr(module, "__name__", "python_schema"),
        "version": int(schema_obj.version),
        "spreadsheet_id": spreadsheet_id,
        "sheets": sheets,
        "value_updates": value_updates,
    }


def _normalize_json_schema(raw: dict[str, Any]) -> dict[str, Any]:
    sheets_raw = raw.get("sheets")
    if not isinstance(sheets_raw, list) or not sheets_raw:
        raise RuntimeError("JSON schema must contain non-empty 'sheets' list")

    normalized_sheets = [_normalize_sheet(dict(item)) for item in sheets_raw]
    schema_obj = SpreadsheetSchema(
        version=int(raw.get("version", 1)),
        sheets=[_sheet_schema_from_dict(sheet) for sheet in normalized_sheets],
    )
    validate_spreadsheet_schema(schema_obj)

    return {
        "name": str(raw.get("name", "json_schema")),
        "version": int(raw.get("version", 1)),
        "spreadsheet_id": raw.get("spreadsheet_id"),
        "sheets": normalized_sheets,
        "value_updates": list(raw.get("value_updates", [])),
    }


def load_schema(schema_path: str | Path) -> dict[str, Any]:
    path = Path(schema_path).expanduser().resolve()
    if not path.exists():
        raise RuntimeError(f"Schema file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise RuntimeError("JSON schema must be an object")
        schema = _normalize_json_schema(raw)
        schema["source_path"] = str(path)
        return schema

    if suffix == ".py":
        module_name = f"findom_schema_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load python schema: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        schema = _normalize_python_schema(module)
        schema["source_path"] = str(path)
        return schema

    raise RuntimeError(f"Unsupported schema format: {path.suffix}")


def build_spreadsheet(
    schema: dict[str, Any],
    spreadsheet_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    target_spreadsheet_id = (spreadsheet_id or schema.get("spreadsheet_id") or "").strip()
    if not target_spreadsheet_id and not dry_run:
        target_spreadsheet_id = require_env("GOOGLE_SHEETS_ID")

    result: dict[str, Any] = {
        "schema": schema.get("name"),
        "schema_version": schema.get("version"),
        "spreadsheet_id": target_spreadsheet_id or "<dry-run>",
        "dry_run": dry_run,
        "sheets": [],
        "value_updates": len(schema.get("value_updates", [])),
    }

    if dry_run:
        for sheet in schema["sheets"]:
            result["sheets"].append(
                {
                    "title": sheet["title"],
                    "columns": len(sheet["columns"]),
                    "clear": bool(sheet.get("clear") or sheet.get("clear_range")),
                    "formulas": len(sheet.get("formulas", [])),
                    "formats": len(sheet.get("number_formats", []))
                    + len(sheet.get("text_formats", []))
                    + len(sheet.get("conditional_formats", [])),
                    "demo_rows": len(sheet.get("demo_rows", [])),
                    "snapshot_rows": len((sheet.get("snapshot") or {}).get("rows", [])),
                }
            )
        return result

    from gsheets_toolkit.client import SheetsClient
    from gsheets_toolkit.ops import ensure_sheet

    client = SheetsClient(spreadsheet_id=target_spreadsheet_id)

    for sheet in schema["sheets"]:
        title = sheet["title"]
        column_count = _column_count(sheet)

        ensure_sheet(
            client,
            sheet_title=title,
            min_rows=max(1, int(sheet.get("min_rows", 1000))),
            min_cols=max(1, int(sheet.get("min_cols", max(26, column_count)))),
        )

        sheet_ids = _sheet_id_by_title(client)
        sheet_id = sheet_ids.get(title)
        if sheet_id is None:
            raise RuntimeError(f"Failed to resolve sheet id for {title}")

        _maybe_clear_sheet(client, sheet)

        updates = _header_updates_for_sheet(sheet)
        if updates:
            client.batch_update_values(updates)

        requests: list[dict[str, Any]] = []
        requests.extend(_grid_requests(sheet, sheet_id=sheet_id, column_count=column_count))
        demo_request = _demo_rows_request(sheet_id, sheet)
        if demo_request is not None:
            requests.append(demo_request)
        requests.extend(_formula_requests(sheet_id, title, list(sheet.get("formulas", []))))
        requests.extend(_number_format_requests(sheet_id, title, list(sheet.get("number_formats", []))))
        requests.extend(_text_format_requests(sheet_id, title, list(sheet.get("text_formats", []))))
        requests.extend(_conditional_format_requests(sheet_id, list(sheet.get("conditional_formats", []))))
        requests.extend(list(sheet.get("batch_requests", [])))

        if requests:
            client.batch_update_requests(requests)

        snapshot_rows = _apply_snapshot_load(client, sheet)
        result["sheets"].append(
            {
                "title": title,
                "columns": column_count,
                "demo_rows": len(sheet.get("demo_rows", [])),
                "snapshot_rows": snapshot_rows,
                "requests": len(requests),
            }
        )

    value_updates = list(schema.get("value_updates", []))
    if value_updates:
        client.batch_update_values(value_updates)

    return result
