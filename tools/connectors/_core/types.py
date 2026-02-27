from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class Account:
    provider: str
    account_id: str
    title: str
    currency: str
    balance: Decimal | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "account_id": self.account_id,
            "title": self.title,
            "currency": self.currency,
            "balance": str(self.balance) if self.balance is not None else None,
            "meta": self.meta,
        }


@dataclass(slots=True)
class Movement:
    provider: str
    external_id: str
    account_id: str
    occurred_at: datetime
    amount: Decimal
    currency: str
    merchant: str | None = None
    comment: str | None = None
    transfer_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "external_id": self.external_id,
            "account_id": self.account_id,
            "occurred_at": self.occurred_at.isoformat(),
            "amount": str(self.amount),
            "currency": self.currency,
            "merchant": self.merchant,
            "comment": self.comment,
            "transfer_id": self.transfer_id,
            "meta": self.meta,
        }


@dataclass(slots=True)
class ConnectorResult:
    accounts: list[Account]
    movements: list[Movement]

    def to_dict(self) -> dict[str, Any]:
        return {
            "accounts": [account.to_dict() for account in self.accounts],
            "movements": [movement.to_dict() for movement in self.movements],
        }


@dataclass(slots=True, frozen=True)
class SyncWindow:
    from_date: date
    to_date: date

    def __post_init__(self) -> None:
        if self.from_date > self.to_date:
            raise ValueError("from_date must be <= to_date")

    def to_dict(self) -> dict[str, str]:
        return {
            "from_date": self.from_date.isoformat(),
            "to_date": self.to_date.isoformat(),
        }
