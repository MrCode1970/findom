from __future__ import annotations

import json
import os
import sys
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv()

from gsheets_toolkit.client import SheetsClient
from gsheets_toolkit.upsert import upsert_rows_snapshot
from gsheets_toolkit.utils import require_env


DEFAULT_MOVEMENTS_JSON = REPO_ROOT / "tests" / "fixtures" / "movements_sample.json"


def _load_json(path: Path) -> Any:
    if not path.exists():
        raise RuntimeError(f"Movements file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in {path}: {exc}") from exc


def _movements_path() -> Path:
    override = os.getenv("MOVEMENTS_JSON", "").strip()
    if not override:
        return DEFAULT_MOVEMENTS_JSON.resolve()
    return Path(override).expanduser().resolve()


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _as_number(value: Any, *, field_name: str, external_id: str) -> float:
    if value is None:
        raise RuntimeError(f"Movement item '{external_id}' has empty {field_name}")
    if isinstance(value, bool):
        raise RuntimeError(f"Movement item '{external_id}' has invalid {field_name}: {value}")
    if isinstance(value, (int, float, Decimal)):
        return float(value)

    text = str(value).strip()
    if text.startswith("'"):
        text = text[1:].strip()
    if not text:
        raise RuntimeError(f"Movement item '{external_id}' has empty {field_name}")

    normalized_text = text.replace(" ", "").replace(",", ".")
    try:
        return float(Decimal(normalized_text))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(
            f"Movement item '{external_id}' has non-numeric {field_name}: {value}"
        ) from exc


def _deduplicate_by_external_id(movements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result_reversed: list[dict[str, Any]] = []
    for item in reversed(movements):
        external_id = _as_str(item.get("external_id")).strip()
        if not external_id:
            raise RuntimeError("Movement item has empty external_id")
        if external_id in seen:
            continue
        seen.add(external_id)
        result_reversed.append(item)
    return list(reversed(result_reversed))


def _to_journal_rows(movements: list[dict[str, Any]], synced_at: str) -> tuple[list[list[Any]], list[str]]:
    rows: list[list[Any]] = []
    keys: list[str] = []
    for movement in movements:
        external_id = _as_str(movement.get("external_id")).strip()
        if not external_id:
            raise RuntimeError("Movement item has empty external_id")
        row = [
            _as_str(movement.get("date")),
            _as_number(movement.get("amount"), field_name="amount", external_id=external_id),
            _as_str(movement.get("currency")),
            _as_str(movement.get("merchant")),
            _as_str(movement.get("comment")),
            _as_str(movement.get("account_id")),
            _as_str(movement.get("source")),
            external_id,
            synced_at,
        ]
        rows.append(row)
        keys.append(external_id)
    return rows, keys


def _update_meta(client: SheetsClient, synced_at: str, row_count: int) -> None:
    client.batch_update_values(
        [
            {
                "range": "_Meta!A2:B4",
                "values": [
                    ["last_sync_at", synced_at],
                    ["row_count", str(row_count)],
                    ["schema_version", "1"],
                ],
            }
        ]
    )


def write_log_snapshot(
    spreadsheet_id: str,
    movements: list[dict[str, Any]],
    synced_at_iso: str,
) -> None:
    if not spreadsheet_id.strip():
        raise RuntimeError("spreadsheet_id is required")
    if not isinstance(movements, list):
        raise RuntimeError("movements must be a list")
    if not all(isinstance(item, dict) for item in movements):
        raise RuntimeError("movements items must be objects")

    normalized = _deduplicate_by_external_id(movements)
    rows, keys = _to_journal_rows(normalized, synced_at_iso)

    client = SheetsClient(spreadsheet_id=spreadsheet_id)
    upsert_rows_snapshot(
        client=client,
        sheet_title="Journal",
        key_col_letter="H",
        columns_span="A:I",
        start_row=2,
        rows=rows,
        keys=keys,
        numeric_column_indexes=[1],  # B: amount
        locale_aware_numeric=True,
    )
    _update_meta(client, synced_at=synced_at_iso, row_count=len(rows))


def main() -> int:
    try:
        spreadsheet_id = require_env("GOOGLE_SHEETS_ID")
        require_env("GOOGLE_APPLICATION_CREDENTIALS")
        movements_path = _movements_path()
        payload = _load_json(movements_path)
        if not isinstance(payload, list):
            raise RuntimeError("Movements JSON must contain a list")
        if not all(isinstance(item, dict) for item in payload):
            raise RuntimeError("Each movement item must be an object")

        synced_at = datetime.now(timezone.utc).isoformat()
        write_log_snapshot(
            spreadsheet_id=spreadsheet_id,
            movements=payload,
            synced_at_iso=synced_at,
        )
        dedup_count = len(_deduplicate_by_external_id(payload))

        print(f"OK: synced {dedup_count} rows")
        print(f"Spreadsheet: {spreadsheet_id}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
