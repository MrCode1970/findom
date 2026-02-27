from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.connectors._core.normalize.dates import parse_date
from tools.connectors._core.storage import load_state, save_state
from tools.connectors._core.types import SyncWindow


def _build_window(state: dict[str, Any]) -> SyncWindow:
    today = date.today()
    last_sync = state.get("last_sync")

    if last_sync:
        from_date = parse_date(last_sync)
    else:
        from_date = today - timedelta(days=30)

    return SyncWindow(from_date=from_date, to_date=today)


def _run_provider(provider: str, window: SyncWindow, state: dict[str, Any]) -> dict[str, Any]:
    api_module = importlib.import_module(f"tools.connectors.providers.{provider}.api")
    converters_module = importlib.import_module(
        f"tools.connectors.providers.{provider}.converters"
    )

    raw = api_module.sync(window, state)
    state_update: dict[str, Any] = {}
    if isinstance(raw, dict):
        maybe_state = raw.get("_state_update")
        if isinstance(maybe_state, dict):
            state_update = maybe_state

    result = converters_module.convert(raw)

    return {
        "provider": provider,
        "window": window.to_dict(),
        "accounts": [account.to_dict() for account in result.accounts],
        "movements": [movement.to_dict() for movement in result.movements],
        "_state_update": state_update,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run connector synchronization")
    parser.add_argument(
        "--provider",
        required=True,
        help="Provider name under tools/connectors/providers",
    )
    args = parser.parse_args()

    state = load_state(args.provider)
    window = _build_window(state)

    payload = _run_provider(args.provider, window, state)
    state_update = payload.pop("_state_update", {})
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    new_state = dict(state)
    if isinstance(state_update, dict):
        new_state.update(state_update)
    if not isinstance(state_update, dict) or "last_sync" not in state_update:
        new_state["last_sync"] = window.to_date.isoformat()
    save_state(args.provider, new_state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
