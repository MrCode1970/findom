from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


MONEY_PLACES = Decimal("0.01")


def parse_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    text = str(value).strip().replace(" ", "").replace(",", ".")
    if not text:
        raise ValueError("Amount value is empty")
    return Decimal(text)


def quantize_money(amount: Decimal, places: Decimal = MONEY_PLACES) -> Decimal:
    return amount.quantize(places, rounding=ROUND_HALF_UP)


def signed_amount(amount: Any, direction: str | None = None) -> Decimal:
    normalized = quantize_money(parse_decimal(amount))
    if direction is None:
        return normalized

    marker = direction.strip().lower()
    if marker in {"debit", "out", "expense", "withdrawal", "dr", "-"}:
        return -abs(normalized)
    if marker in {"credit", "in", "income", "deposit", "cr", "+"}:
        return abs(normalized)

    raise ValueError(f"Unsupported movement direction: {direction}")
