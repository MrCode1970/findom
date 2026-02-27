from __future__ import annotations

from typing import Any

from tools.connectors._core.normalize.dates import parse_date
from tools.connectors._core.types import SyncWindow
from tools.connectors.providers.manual_csv.fetch import fetch_rows


def sync(window: SyncWindow, state: dict[str, Any]) -> dict[str, Any]:
    rows = fetch_rows()
    filtered_rows = [
        row for row in rows if window.from_date <= parse_date(row["date"]) <= window.to_date
    ]
    return {"rows": filtered_rows, "state": state}
