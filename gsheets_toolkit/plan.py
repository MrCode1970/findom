from __future__ import annotations

from dataclasses import dataclass

from gsheets_toolkit.utils import parse_columns_span


@dataclass(slots=True)
class RowUpdate:
    row_index: int
    values: list[str]


def group_row_updates(
    sheet_title: str,
    columns_span: str,
    updates: list[RowUpdate],
) -> list[dict]:
    if not updates:
        return []

    left_col, right_col = parse_columns_span(columns_span)
    ordered = sorted(updates, key=lambda item: item.row_index)

    blocks: list[dict] = []
    block_start = ordered[0].row_index
    prev_row = ordered[0].row_index
    block_values = [ordered[0].values]

    for item in ordered[1:]:
        if item.row_index == prev_row + 1:
            block_values.append(item.values)
            prev_row = item.row_index
            continue

        blocks.append(
            {
                "range": f"{sheet_title}!{left_col}{block_start}:{right_col}{prev_row}",
                "values": block_values,
            }
        )
        block_start = item.row_index
        prev_row = item.row_index
        block_values = [item.values]

    blocks.append(
        {
            "range": f"{sheet_title}!{left_col}{block_start}:{right_col}{prev_row}",
            "values": block_values,
        }
    )
    return blocks

