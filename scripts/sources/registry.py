from __future__ import annotations

import importlib
from datetime import date, datetime
from typing import Any

from tools.connectors._core.storage import load_state
from tools.connectors._core.types import SyncWindow


def _run_cal_digital(from_date: date, to_date: date) -> tuple[list[dict], list[dict]]:
    api_module = importlib.import_module("tools.connectors.providers.cal_digital.api")
    converters_module = importlib.import_module("tools.connectors.providers.cal_digital.converters")
    state = load_state("cal_digital")
    window = SyncWindow(from_date=from_date, to_date=to_date)

    raw = api_module.sync(window, state)
    result = converters_module.convert(raw)
    return [item.to_dict() for item in result.accounts], [item.to_dict() for item in result.movements]


def _run_discount(_from_date: date, _to_date: date) -> tuple[list[dict], list[dict]]:
    raise NotImplementedError("discount not wired yet")


SOURCE_RUNNERS: dict[str, Any] = {
    "cal_digital": _run_cal_digital,
    "discount": _run_discount,
}


def _normalize_iso_datetime(value: str, *, field_name: str, source: str, index: int) -> str:
    text = value.strip()
    if not text:
        raise RuntimeError(f"Source {source} movement[{index}] has empty {field_name}")
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(
            f"Source {source} movement[{index}] has non-ISO {field_name}: {text}"
        ) from exc
    return text


def _normalize_required_str(
    movement: dict[str, Any],
    key: str,
    *,
    source: str,
    index: int,
) -> str:
    value = movement.get(key)
    text = "" if value is None else str(value).strip()
    if not text:
        raise RuntimeError(f"Source {source} movement[{index}] is missing required field '{key}'")
    return text


def _normalize_movement_for_log(
    movement: dict[str, Any],
    *,
    source: str,
    index: int,
) -> dict[str, str]:
    if not isinstance(movement, dict):
        raise RuntimeError(f"Source {source} movement[{index}] must be an object")

    date_value = movement.get("date")
    if date_value in (None, ""):
        date_value = movement.get("occurred_at")
    date_text = _normalize_iso_datetime(
        str(date_value) if date_value is not None else "",
        field_name="date/occurred_at",
        source=source,
        index=index,
    )

    normalized = {
        "date": date_text,
        "amount": _normalize_required_str(movement, "amount", source=source, index=index),
        "currency": _normalize_required_str(movement, "currency", source=source, index=index),
        "merchant": "" if movement.get("merchant") is None else str(movement.get("merchant")),
        "comment": "" if movement.get("comment") is None else str(movement.get("comment")),
        "account_id": _normalize_required_str(movement, "account_id", source=source, index=index),
        "source": _normalize_required_str(movement, "source", source=source, index=index),
        "external_id": _normalize_required_str(movement, "external_id", source=source, index=index),
    }
    return normalized


def _normalize_movements_for_log(movements: list[dict], source: str) -> list[dict]:
    return [
        _normalize_movement_for_log(item, source=source, index=index)
        for index, item in enumerate(movements, start=1)
    ]


def run_sources(sources: list[str], from_date: date, to_date: date) -> tuple[list[dict], list[dict]]:
    accounts: list[dict] = []
    movements: list[dict] = []

    for source in sources:
        runner = SOURCE_RUNNERS.get(source)
        if runner is None:
            raise RuntimeError(f"Unknown source: {source}")
        try:
            source_accounts, source_movements = runner(from_date, to_date)
        except Exception as exc:
            raise RuntimeError(f"Source {source} failed: {exc}") from exc

        if not isinstance(source_accounts, list) or not isinstance(source_movements, list):
            raise RuntimeError(f"Source {source} returned invalid payload format")
        accounts.extend(source_accounts)
        movements.extend(_normalize_movements_for_log(source_movements, source))

    return accounts, movements
