from __future__ import annotations

import hashlib
from typing import Any

from tools.connectors._core.normalize.dates import parse_datetime
from tools.connectors._core.normalize.money import signed_amount
from tools.connectors._core.normalize.text import clean_comment, clean_merchant, clean_text
from tools.connectors._core.types import Account, ConnectorResult, Movement


PROVIDER_NAME = "manual_csv"


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
    accounts_by_id: dict[str, Account] = {}
    movements: list[Movement] = []

    for row in raw.get("rows", []):
        account_id = clean_text(row.get("account_id")) or "unknown"
        account_title = clean_text(row.get("account_name")) or account_id
        currency = (clean_text(row.get("currency")) or "USD").upper()

        if account_id not in accounts_by_id:
            accounts_by_id[account_id] = Account(
                provider=PROVIDER_NAME,
                account_id=account_id,
                title=account_title,
                currency=currency,
            )

        occurred_at = parse_datetime(row["date"])
        amount = signed_amount(row.get("amount", "0"), row.get("direction"))
        merchant = clean_merchant(row.get("merchant"))
        comment = clean_comment(row.get("comment"))

        transfer_group = clean_text(row.get("transfer_group"))
        transfer_id = transfer_group or None

        external_id = _build_external_id(
            occurred_date=occurred_at.date().isoformat(),
            amount=f"{amount:.2f}",
            account_id=account_id,
            merchant=merchant,
            comment=comment,
        )

        movements.append(
            Movement(
                provider=PROVIDER_NAME,
                external_id=external_id,
                account_id=account_id,
                occurred_at=occurred_at,
                amount=amount,
                currency=currency,
                merchant=merchant,
                comment=comment,
                transfer_id=transfer_id,
            )
        )

    return ConnectorResult(accounts=list(accounts_by_id.values()), movements=movements)
