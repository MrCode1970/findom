from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gsheets_toolkit.client import SheetsClient
from gsheets_toolkit.ops import apply_sheet_schema, ensure_sheet
from gsheets_toolkit.schema import validate_spreadsheet_schema
from gsheets_toolkit.utils import require_env
from tables.log_schema import LOG_SPREADSHEET_SCHEMA, META_SHEET


def main() -> int:
    try:
        spreadsheet_id = require_env("GOOGLE_SHEETS_ID")
        validate_spreadsheet_schema(LOG_SPREADSHEET_SCHEMA)
        client = SheetsClient(spreadsheet_id=spreadsheet_id)

        for sheet in LOG_SPREADSHEET_SCHEMA.sheets:
            ensure_sheet(
                client,
                sheet_title=sheet.title,
                min_rows=1000,
                min_cols=max(26, len(sheet.columns)),
            )
            apply_sheet_schema(client, sheet)

        client.batch_update_values(
            [
                {
                    "range": f"{META_SHEET.title}!A2:B3",
                    "values": [
                        ["last_sync_at", ""],
                        ["schema_version", str(LOG_SPREADSHEET_SCHEMA.version)],
                    ],
                }
            ]
        )

        print("OK: log table ready")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

