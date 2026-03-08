from gsheets_toolkit.models import ColumnSpec, SheetSchema, SpreadsheetSchema

__all__ = [
    "ColumnSpec",
    "SheetSchema",
    "SpreadsheetSchema",
]

try:
    from gsheets_toolkit.client import SheetsClient
    from gsheets_toolkit.ops import apply_sheet_schema, ensure_sheet
    from gsheets_toolkit.upsert import load_key_index, upsert_rows_snapshot

    __all__.extend(
        [
            "SheetsClient",
            "ensure_sheet",
            "apply_sheet_schema",
            "load_key_index",
            "upsert_rows_snapshot",
        ]
    )
except RuntimeError:
    # Allow importing schema/model types without installed Google SDK deps.
    pass
