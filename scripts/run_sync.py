from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv()

from scripts.sources.registry import run_sources


def _parse_iso_date(value: str, flag: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f"{flag} must be in YYYY-MM-DD format") from exc


def _resolve_window(from_raw: str | None, to_raw: str | None) -> tuple[date, date]:
    today_local = datetime.now().astimezone().date()

    if from_raw and to_raw:
        from_date = _parse_iso_date(from_raw, "--from-date")
        to_date = _parse_iso_date(to_raw, "--to-date")
    elif from_raw and not to_raw:
        from_date = _parse_iso_date(from_raw, "--from-date")
        to_date = today_local
    elif to_raw and not from_raw:
        to_date = _parse_iso_date(to_raw, "--to-date")
        from_date = to_date - timedelta(days=30)
    else:
        to_date = today_local
        from_date = today_local - timedelta(days=30)

    if from_date > to_date:
        raise RuntimeError("from_date must be <= to_date")
    return from_date, to_date


def _find_duplicate_external_ids(movements: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    dup_seen: set[str] = set()
    for item in movements:
        ext_id = str(item.get("external_id", "")).strip()
        if not ext_id:
            continue
        if ext_id in seen and ext_id not in dup_seen:
            duplicates.append(ext_id)
            dup_seen.add(ext_id)
        seen.add(ext_id)
    return duplicates


def _print_summary(
    sources: list[str],
    from_date: date,
    to_date: date,
    accounts_count: int,
    movements_count: int,
    write_log: bool,
    dry_run: bool,
) -> None:
    write_line = "NO (dry-run)" if dry_run else ("YES" if write_log else "NO")
    print(f"Sources: {','.join(sources)}")
    print(f"Window:  {from_date.isoformat()} -> {to_date.isoformat()}")
    print(f"Accounts: {accounts_count}")
    print(f"Movements: {movements_count}")
    print(f"Write log: {write_line}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run findom sources and optionally write log snapshot")
    parser.add_argument("--source", action="append", default=[], help="Source name (repeatable)")
    parser.add_argument("--from-date", dest="from_date", default=None, help="Window start YYYY-MM-DD")
    parser.add_argument("--to-date", dest="to_date", default=None, help="Window end YYYY-MM-DD")
    parser.add_argument("--write-log", action="store_true", help="Write movements snapshot to Google Sheets")
    parser.add_argument("--dry-run", action="store_true", help="Run sources without writing to Google Sheets")
    args = parser.parse_args()

    try:
        sources = [item.strip() for item in args.source if item and item.strip()]
        if not sources:
            print("ERROR: at least one --source is required", file=sys.stderr)
            return 2

        from_date, to_date = _resolve_window(args.from_date, args.to_date)
        accounts, movements = run_sources(sources=sources, from_date=from_date, to_date=to_date)

        duplicates = _find_duplicate_external_ids(movements)
        if duplicates:
            print(
                f"ERROR: duplicate external_id values detected: {len(duplicates)} "
                f"(first 5: {', '.join(duplicates[:5])})",
                file=sys.stderr,
            )
            return 2

        synced_at_iso = datetime.now().astimezone().isoformat()

        if args.write_log and not args.dry_run:
            spreadsheet_id = os.getenv("GOOGLE_SHEETS_ID", "").strip()
            if not spreadsheet_id:
                raise RuntimeError("GOOGLE_SHEETS_ID is required when --write-log is used")
            writer_module = import_module("scripts.sync_log_to_sheet")
            write_log_snapshot = getattr(writer_module, "write_log_snapshot")
            write_log_snapshot(
                spreadsheet_id=spreadsheet_id,
                movements=movements,
                synced_at_iso=synced_at_iso,
            )

        _print_summary(
            sources=sources,
            from_date=from_date,
            to_date=to_date,
            accounts_count=len(accounts),
            movements_count=len(movements),
            write_log=args.write_log,
            dry_run=args.dry_run,
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
