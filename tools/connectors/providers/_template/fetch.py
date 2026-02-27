from __future__ import annotations

import json
from pathlib import Path

from tools.connectors._core.normalize.dates import parse_date
from tools.connectors._core.types import SyncWindow


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> list[dict]:
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def fetch_accounts() -> list[dict]:
    return _load_fixture("accounts.json")


def fetch_transactions(window: SyncWindow) -> list[dict]:
    records = _load_fixture("transactions.json")
    return [
        record
        for record in records
        if window.from_date <= parse_date(record["date"]) <= window.to_date
    ]
