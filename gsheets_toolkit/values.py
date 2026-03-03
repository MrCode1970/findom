from __future__ import annotations

from decimal import Decimal
from typing import Any

from gsheets_toolkit.utils import column_letter_to_index, parse_columns_span


def to_cell_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def normalize_rows(rows: list[list[Any]], columns_span: str) -> list[list[str]]:
    left, right = parse_columns_span(columns_span)
    width = column_letter_to_index(right) - column_letter_to_index(left) + 1
    if width <= 0:
        raise ValueError(f"Invalid columns span width: {columns_span}")

    normalized: list[list[str]] = []
    for row in rows:
        converted = [to_cell_str(cell) for cell in row[:width]]
        if len(converted) < width:
            converted.extend("" for _ in range(width - len(converted)))
        normalized.append(converted)
    return normalized

