from __future__ import annotations

from decimal import Decimal
from typing import Any

from gsheets_toolkit.utils import column_letter_to_index, parse_columns_span


def to_cell_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def normalize_rows(rows: list[list[Any]], columns_span: str) -> list[list[Any]]:
    left, right = parse_columns_span(columns_span)
    width = column_letter_to_index(right) - column_letter_to_index(left) + 1
    if width <= 0:
        raise ValueError(f"Invalid columns span width: {columns_span}")

    normalized: list[list[Any]] = []
    for row in rows:
        converted = [to_cell_value(cell) for cell in row[:width]]
        if len(converted) < width:
            converted.extend("" for _ in range(width - len(converted)))
        normalized.append(converted)
    return normalized


def is_russian_locale(locale: str) -> bool:
    text = locale.strip().lower()
    return text.startswith("ru")


def locale_aware_numeric_cell(value: Any, locale: str) -> Any:
    if not is_russian_locale(locale):
        return value
    if isinstance(value, bool):
        return value
    if not isinstance(value, (int, float, Decimal)):
        return value
    rendered = format(Decimal(str(value)), "f")
    return rendered.replace(".", ",")


def locale_aware_numeric_rows(
    rows: list[list[Any]],
    locale: str,
    numeric_column_indexes: set[int],
) -> list[list[Any]]:
    if not numeric_column_indexes or not is_russian_locale(locale):
        return rows

    converted_rows: list[list[Any]] = []
    for row in rows:
        converted_row: list[Any] = []
        for idx, cell in enumerate(row):
            if idx in numeric_column_indexes:
                converted_row.append(locale_aware_numeric_cell(cell, locale))
            else:
                converted_row.append(cell)
        converted_rows.append(converted_row)
    return converted_rows
