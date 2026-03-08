from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tables.spreadsheet_builder import build_spreadsheet, load_schema


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update Google Spreadsheet from schema")
    parser.add_argument("--schema", required=True, help="Path to schema file (.py or .json)")
    parser.add_argument("--spreadsheet-id", default="", help="Existing spreadsheet id (optional)")
    parser.add_argument("--dry-run", action="store_true", help="Show planned actions without API calls")
    return parser.parse_args()


def main() -> int:
    try:
        args = _parse_args()
        schema = load_schema(args.schema)
        result = build_spreadsheet(
            schema=schema,
            spreadsheet_id=args.spreadsheet_id.strip() or None,
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
