from __future__ import annotations

import json
from pathlib import Path
from typing import Any


STATE_DIR = Path(__file__).resolve().parents[3] / "findom" / ".state"


def _state_path(connector_name: str) -> Path:
    safe_name = connector_name.strip().lower().replace("/", "_")
    return STATE_DIR / f"{safe_name}.json"


def load_state(connector_name: str) -> dict[str, Any]:
    path = _state_path(connector_name)
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def save_state(connector_name: str, state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = _state_path(connector_name)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2, sort_keys=True)
