from __future__ import annotations

import importlib
from decimal import Decimal, InvalidOperation
from datetime import date, datetime
from typing import Any

from tools.connectors._core.storage import load_state
from tools.connectors._core.types import SyncWindow


def _print_cal_diagnostics(raw: dict[str, Any], conversion_diag: dict[str, Any]) -> None:
    history_diag = raw.get("history_diagnostics", {})
    totals = history_diag.get("totals", {}) if isinstance(history_diag, dict) else {}

    clearance_raw = int(totals.get("clearance_raw", 0))
    filtered_raw = int(totals.get("filtered_raw", 0))
    merged_unique = int(totals.get("merged_unique", 0))
    dedup_dropped = max(0, (clearance_raw + filtered_raw) - merged_unique)
    converted_movements = int(conversion_diag.get("converted_movements", 0))
    skipped_in_converter = int(conversion_diag.get("skipped_in_converter", 0))

    print(
        "[cal_digital][diag] "
        f"clearance_raw={clearance_raw} "
        f"filtered_raw={filtered_raw} "
        f"merged_unique={merged_unique} "
        f"dedup_dropped_count={dedup_dropped} "
        f"converted_movements={converted_movements} "
        f"skipped_in_converter={skipped_in_converter}"
    )

    per_card = history_diag.get("per_card", {}) if isinstance(history_diag, dict) else {}
    if isinstance(per_card, dict) and per_card:
        for card_id, stats in sorted(per_card.items()):
            if not isinstance(stats, dict):
                continue
            print(
                "[cal_digital][diag][per_card] "
                f"card={card_id} "
                f"clearance_raw={int(stats.get('clearance_raw', 0))} "
                f"filtered_raw={int(stats.get('filtered_raw', 0))} "
                f"merged_unique={int(stats.get('merged_unique', 0))}"
            )

    skip_reasons = conversion_diag.get("skip_reasons", {})
    if isinstance(skip_reasons, dict) and skip_reasons:
        parts = [f"{key}={int(value)}" for key, value in sorted(skip_reasons.items())]
        print("[cal_digital][diag][skip_reasons] " + " ".join(parts))


def _run_cal_digital(from_date: date, to_date: date) -> tuple[list[dict], list[dict]]:
    api_module = importlib.import_module("tools.connectors.providers.cal_digital.api")
    converters_module = importlib.import_module("tools.connectors.providers.cal_digital.converters")
    state = load_state("cal_digital")
    window = SyncWindow(from_date=from_date, to_date=to_date)

    raw = api_module.sync(window, state)
    convert_with_diagnostics = getattr(converters_module, "convert_with_diagnostics", None)
    if callable(convert_with_diagnostics):
        result, conversion_diag = convert_with_diagnostics(raw)
    else:
        result = converters_module.convert(raw)
        conversion_diag = {"converted_movements": len(result.movements), "skipped_in_converter": 0}

    _print_cal_diagnostics(raw, conversion_diag)
    return [item.to_dict() for item in result.accounts], [item.to_dict() for item in result.movements]


def _run_discount(_from_date: date, _to_date: date) -> tuple[list[dict], list[dict]]:
    api_module = importlib.import_module("tools.connectors.providers.discount.api")
    converters_module = importlib.import_module("tools.connectors.providers.discount.converters")
    state = load_state("discount")
    window = SyncWindow(from_date=_from_date, to_date=_to_date)

    raw = api_module.sync(window, state)
    result = converters_module.convert(raw)
    return [item.to_dict() for item in result.accounts], [item.to_dict() for item in result.movements]


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


def _normalize_required_str_from_keys(
    movement: dict[str, Any],
    keys: tuple[str, ...],
    *,
    source: str,
    index: int,
    field_name: str,
) -> str:
    for key in keys:
        value = movement.get(key)
        text = "" if value is None else str(value).strip()
        if text:
            return text
    raise RuntimeError(f"Source {source} movement[{index}] is missing required field '{field_name}'")


def _normalize_required_amount(
    movement: dict[str, Any],
    *,
    source: str,
    index: int,
) -> float:
    raw = movement.get("amount")
    if raw is None:
        raise RuntimeError(f"Source {source} movement[{index}] is missing required field 'amount'")

    if isinstance(raw, bool):
        raise RuntimeError(f"Source {source} movement[{index}] has invalid amount: {raw}")

    if isinstance(raw, (int, float, Decimal)):
        return float(raw)

    text = str(raw).strip()
    if text.startswith("'"):
        text = text[1:].strip()
    if not text:
        raise RuntimeError(f"Source {source} movement[{index}] has empty amount")

    normalized_text = text.replace(" ", "").replace(",", ".")
    try:
        return float(Decimal(normalized_text))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(
            f"Source {source} movement[{index}] has non-numeric amount: {raw}"
        ) from exc


def _normalize_movement_for_log(
    movement: dict[str, Any],
    *,
    source: str,
    index: int,
) -> dict[str, Any]:
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
        "amount": _normalize_required_amount(movement, source=source, index=index),
        "currency": _normalize_required_str(movement, "currency", source=source, index=index),
        "merchant": "" if movement.get("merchant") is None else str(movement.get("merchant")),
        "comment": "" if movement.get("comment") is None else str(movement.get("comment")),
        "account_id": _normalize_required_str(movement, "account_id", source=source, index=index),
        "source": _normalize_required_str_from_keys(
            movement,
            ("source", "provider"),
            source=source,
            index=index,
            field_name="source/provider",
        ),
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
