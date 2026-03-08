from __future__ import annotations

from typing import Any

from gsheets_toolkit.client import SheetsClient
from gsheets_toolkit.plan import RowUpdate, group_row_updates
from gsheets_toolkit.utils import parse_columns_span
from gsheets_toolkit.values import locale_aware_numeric_rows, normalize_rows


_INDEX_STATS: dict[tuple[str, str, int], dict[str, int]] = {}


def load_key_index(
    client: SheetsClient,
    sheet_title: str,
    key_col_letter: str,
    start_row: int = 2,
) -> dict[str, int]:
    key_range = f"{sheet_title}!{key_col_letter}{start_row}:{key_col_letter}"
    values = client.get_values(key_range)

    key_index: dict[str, int] = {}
    duplicate_keys_count = 0
    for offset, row in enumerate(values):
        if not row:
            continue
        key = str(row[0]).strip()
        if not key:
            continue
        if key in key_index:
            duplicate_keys_count += 1
        key_index[key] = start_row + offset

    max_row_by_values = start_row - 1 + len(values) if values else start_row - 1
    max_row_by_keys = max(key_index.values()) if key_index else start_row - 1
    _INDEX_STATS[(sheet_title, key_col_letter.upper(), start_row)] = {
        "old_count": max(max_row_by_values, max_row_by_keys),
        "duplicate_keys_count": duplicate_keys_count,
    }
    return key_index


def _old_count_for(
    sheet_title: str,
    key_col_letter: str,
    start_row: int,
) -> int:
    stats = _INDEX_STATS.get((sheet_title, key_col_letter.upper(), start_row), {})
    return int(stats.get("old_count", start_row - 1))


def _duplicate_keys_count_for(
    sheet_title: str,
    key_col_letter: str,
    start_row: int,
) -> int:
    stats = _INDEX_STATS.get((sheet_title, key_col_letter.upper(), start_row), {})
    return int(stats.get("duplicate_keys_count", 0))


def upsert_rows_snapshot(
    client: SheetsClient,
    sheet_title: str,
    key_col_letter: str,
    columns_span: str,
    start_row: int,
    rows: list[list[Any]],
    keys: list[str],
    numeric_column_indexes: list[int] | None = None,
    locale_aware_numeric: bool = False,
) -> None:
    if len(rows) != len(keys):
        raise ValueError("rows and keys length mismatch")
    if len(set(keys)) != len(keys):
        raise ValueError("keys must be unique in one snapshot run")

    normalized_rows = normalize_rows(rows, columns_span)
    value_input_option = "RAW"
    effective_rows = normalized_rows
    numeric_indexes = {int(idx) for idx in (numeric_column_indexes or []) if int(idx) >= 0}
    if locale_aware_numeric and numeric_indexes:
        locale = client.get_spreadsheet_locale()
        effective_rows = locale_aware_numeric_rows(normalized_rows, locale, numeric_indexes)
        if effective_rows is not normalized_rows:
            value_input_option = "USER_ENTERED"
    key_index = load_key_index(client, sheet_title, key_col_letter, start_row=start_row)
    old_count = _old_count_for(sheet_title, key_col_letter, start_row)
    duplicate_keys_count = _duplicate_keys_count_for(sheet_title, key_col_letter, start_row)

    # If the sheet already contains duplicate keys, reset snapshot range once
    # to restore key uniqueness guarantees for future upserts.
    if duplicate_keys_count > 0:
        left_col, right_col = parse_columns_span(columns_span)
        if old_count >= start_row:
            client.clear_values(f"{sheet_title}!{left_col}{start_row}:{right_col}{old_count}")
        if effective_rows:
            append_start = start_row
            append_end = append_start + len(effective_rows) - 1
            client.batch_update_values(
                [
                    {
                        "range": f"{sheet_title}!{left_col}{append_start}:{right_col}{append_end}",
                        "values": effective_rows,
                    }
                ],
                value_input_option=value_input_option,
            )
        return

    existing_updates: list[RowUpdate] = []
    append_rows: list[list[Any]] = []
    for row_values, key in zip(effective_rows, keys):
        existing_row = key_index.get(str(key).strip())
        if existing_row is not None:
            existing_updates.append(RowUpdate(row_index=existing_row, values=row_values))
        else:
            append_rows.append(row_values)

    updates = group_row_updates(sheet_title, columns_span, existing_updates)

    if append_rows:
        left_col, right_col = parse_columns_span(columns_span)
        append_start = max(start_row, old_count + 1)
        append_end = append_start + len(append_rows) - 1
        updates.append(
            {
                "range": f"{sheet_title}!{left_col}{append_start}:{right_col}{append_end}",
                "values": append_rows,
            }
        )

    if updates:
        client.batch_update_values(updates, value_input_option=value_input_option)

    left_col, right_col = parse_columns_span(columns_span)
    new_count = len(effective_rows)
    new_last_row = start_row + new_count - 1

    if new_count == 0:
        if old_count >= start_row:
            client.clear_values(f"{sheet_title}!{left_col}{start_row}:{right_col}{old_count}")
        return

    if old_count > new_last_row:
        clear_from = new_last_row + 1
        client.clear_values(f"{sheet_title}!{left_col}{clear_from}:{right_col}{old_count}")
