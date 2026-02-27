from __future__ import annotations

from typing import Any

from tools.connectors._core.types import SyncWindow
from tools.connectors.providers._template.fetch import fetch_accounts, fetch_transactions


def sync(window: SyncWindow, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "accounts": fetch_accounts(),
        "transactions": fetch_transactions(window),
        "state": state,
    }
