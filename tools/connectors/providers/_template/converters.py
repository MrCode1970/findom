from __future__ import annotations

import hashlib
from typing import Any

from tools.connectors._core.normalize.dates import parse_datetime
from tools.connectors._core.normalize.money import quantize_money, parse_decimal, signed_amount
from tools.connectors._core.normalize.text import clean_comment, clean_merchant, clean_text
from tools.connectors._core.types import Account, ConnectorResult, Movement


PROVIDER_NAME = "_template"


def _build_external_id(
    occurred_date: str,
    amount: str,
    account_id: str,
    merchant: str | None,
    comment: str | None,
) -> str:
    payload = "|".join(
        [
            occurred_date,
            amount,
            account_id,
            merchant or "",
            comment or "",
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def convert(raw: dict[str, Any]) -> ConnectorResult:
    accounts: list[Account] = []
    movements: list[Movement] = []

    for item in raw.get("accounts", []):
        balance = item.get("balance")
        accounts.append(
            Account(
                provider=PROVIDER_NAME,
                account_id=str(item["id"]),
                title=item.get("title", str(item["id"])),
                currency=item.get("currency", "USD").upper(),
                balance=(
                    quantize_money(parse_decimal(balance))
                    if balance is not None and balance != ""
                    else None
                ),
            )
        )

    for item in raw.get("transactions", []):
        account_id = clean_text(item.get("account_id")) or "unknown"
        amount = signed_amount(item.get("amount", "0"), item.get("direction"))
        merchant = clean_merchant(item.get("merchant"))
        comment = clean_comment(item.get("comment"))
        occurred_at = parse_datetime(item["date"])

        external_id = _build_external_id(
            occurred_date=occurred_at.date().isoformat(),
            amount=f"{amount:.2f}",
            account_id=account_id,
            merchant=merchant,
            comment=comment,
        )

        transfer_group = clean_text(item.get("transfer_group"))

        movements.append(
            Movement(
                provider=PROVIDER_NAME,
                external_id=external_id,
                account_id=account_id,
                occurred_at=occurred_at,
                amount=amount,
                currency=item.get("currency", "USD").upper(),
                merchant=merchant,
                comment=comment,
                transfer_id=transfer_group,
            )
        )

    return ConnectorResult(accounts=accounts, movements=movements)
