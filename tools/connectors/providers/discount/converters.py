from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from tools.connectors._core.normalize.money import quantize_money
from tools.connectors._core.normalize.text import clean_comment, clean_merchant, clean_text
from tools.connectors._core.types import Account, ConnectorResult, Movement


PROVIDER_NAME = "discount"
DEFAULT_CURRENCY = "ILS"


def _parse_yyyymmdd(value: Any) -> datetime:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return quantize_money(value)
    text = str(value or "0").replace(",", "").strip()
    try:
        return quantize_money(Decimal(text))
    except Exception:
        return Decimal("0.00")


def _account_number(raw: dict[str, Any]) -> str:
    meta = raw.get("request_meta", {})
    number = str(meta.get("account_number", "")).strip()
    if number:
        return number

    info = raw.get("last_transactions", {}).get("CurrentAccountLastTransactions", {}).get("CurrentAccountInfo", {})
    fallback = str(info.get("AccountNumber", "")).strip()
    return fallback or "unknown"


def _currency(raw: dict[str, Any]) -> str:
    info = raw.get("last_transactions", {}).get("CurrentAccountLastTransactions", {}).get("CurrentAccountInfo", {})
    code = clean_text(info.get("AccountCurrencyCode"))
    if code and len(code) == 3:
        return code.upper()
    return DEFAULT_CURRENCY


def _external_id(account_number: str, row: dict[str, Any], occurred_at: datetime, amount: Decimal) -> str:
    urn = clean_text(row.get("Urn"))
    if urn:
        return f"discount:{account_number}:{urn}"

    raw = "|".join(
        [
            account_number,
            occurred_at.date().isoformat(),
            f"{amount:.2f}",
            clean_text(row.get("OperationDescriptionToDisplay")) or "",
            str(row.get("OperationNumber", "")),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"discount:{account_number}:{digest}"


def convert(raw: dict[str, Any]) -> ConnectorResult:
    payload = raw.get("last_transactions", {})
    root = payload.get("CurrentAccountLastTransactions", {}) if isinstance(payload, dict) else {}
    entries = root.get("OperationEntry", []) if isinstance(root, dict) else []

    account_number = _account_number(raw)
    account_id = f"discount:{account_number}"
    currency = _currency(raw)

    info = root.get("CurrentAccountInfo", {}) if isinstance(root, dict) else {}
    balance_value = info.get("AccountBalance")
    balance = _safe_decimal(balance_value) if balance_value is not None else None

    account = Account(
        provider=PROVIDER_NAME,
        account_id=account_id,
        title=f"Discount account {account_number[-4:]}" if account_number != "unknown" else "Discount account",
        currency=currency,
        balance=balance,
    )

    movements: list[Movement] = []
    for row in entries:
        if not isinstance(row, dict):
            continue

        occurred_at = _parse_yyyymmdd(row.get("OperationDate"))
        amount = _safe_decimal(row.get("OperationAmount"))

        description = clean_text(row.get("OperationDescriptionToDisplay"))
        if not description:
            parts = [
                clean_text(row.get("OperationDescription")) or "",
                clean_text(row.get("OperationDescription2")) or "",
            ]
            description = clean_text(" ".join(part for part in parts if part))

        merchant = clean_merchant(description)
        comment = clean_comment(row.get("EventName") or row.get("CategoryDescription") or description)

        movements.append(
            Movement(
                provider=PROVIDER_NAME,
                external_id=_external_id(account_number, row, occurred_at, amount),
                account_id=account_id,
                occurred_at=occurred_at,
                amount=amount,
                currency=currency,
                merchant=merchant,
                comment=comment,
                meta={
                    "operation_code": row.get("OperationCode"),
                    "operation_number": row.get("OperationNumber"),
                    "urn": row.get("Urn"),
                },
            )
        )

    return ConnectorResult(accounts=[account], movements=movements)
