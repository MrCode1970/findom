from __future__ import annotations

from gsheets_toolkit.models import ColumnSpec, SheetSchema, SpreadsheetSchema


JOURNAL_SHEET = SheetSchema(
    title="Journal",
    columns=[
        ColumnSpec(key="date", header="date", width=130),
        ColumnSpec(key="amount", header="amount", width=110),
        ColumnSpec(key="currency", header="currency", width=90),
        ColumnSpec(key="merchant", header="merchant", width=240),
        ColumnSpec(key="comment", header="comment", width=280),
        ColumnSpec(key="account_id", header="account_id", width=180),
        ColumnSpec(key="source", header="source", width=140),
        ColumnSpec(key="external_id", header="external_id", width=240, hidden=False),
        ColumnSpec(key="synced_at", header="synced_at", width=170),
    ],
    freeze_rows=1,
    filter=True,
)


META_SHEET = SheetSchema(
    title="_Meta",
    columns=[
        ColumnSpec(key="key", header="key", width=180),
        ColumnSpec(key="value", header="value", width=320),
    ],
    freeze_rows=1,
    filter=True,
)


LOG_SPREADSHEET_SCHEMA = SpreadsheetSchema(
    sheets=[JOURNAL_SHEET, META_SHEET],
    version=1,
)


VALUE_UPDATES = [
    {
        "range": f"{META_SHEET.title}!A2:B4",
        "values": [
            ["last_sync_at", ""],
            ["row_count", "0"],
            ["schema_version", str(LOG_SPREADSHEET_SCHEMA.version)],
        ],
    }
]
