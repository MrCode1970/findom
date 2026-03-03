from __future__ import annotations

from gsheets_toolkit.models import SheetSchema, SpreadsheetSchema


def validate_sheet_schema(sheet: SheetSchema) -> None:
    if not sheet.title.strip():
        raise ValueError("Sheet title must be non-empty")
    if not sheet.columns:
        raise ValueError(f"Sheet {sheet.title} must contain at least one column")

    seen_keys: set[str] = set()
    for col in sheet.columns:
        if not col.key.strip():
            raise ValueError(f"Sheet {sheet.title} contains column with empty key")
        if not col.header.strip():
            raise ValueError(f"Sheet {sheet.title} contains column with empty header")
        if col.key in seen_keys:
            raise ValueError(f"Sheet {sheet.title} has duplicate column key: {col.key}")
        seen_keys.add(col.key)
        if col.width is not None and col.width <= 0:
            raise ValueError(f"Column {col.key} width must be positive")


def validate_spreadsheet_schema(schema: SpreadsheetSchema) -> None:
    if schema.version <= 0:
        raise ValueError("Schema version must be > 0")
    if not schema.sheets:
        raise ValueError("Spreadsheet schema must contain at least one sheet")

    seen_titles: set[str] = set()
    for sheet in schema.sheets:
        validate_sheet_schema(sheet)
        if sheet.title in seen_titles:
            raise ValueError(f"Duplicate sheet title in schema: {sheet.title}")
        seen_titles.add(sheet.title)


def headers(sheet: SheetSchema) -> list[str]:
    return [col.header for col in sheet.columns]

