from __future__ import annotations

import csv
from pathlib import Path


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def fetch_rows(csv_path: Path | None = None) -> list[dict[str, str]]:
    path = csv_path or (FIXTURES_DIR / "sample.csv")
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]
