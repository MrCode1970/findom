from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from tools.connectors._core.normalize.dates import parse_datetime
from tools.connectors._core.normalize.money import parse_decimal, quantize_money
from tools.connectors._core.normalize.text import clean_comment, clean_merchant, clean_text
from tools.connectors._core.types import Account, ConnectorResult, Movement


PROVIDER_NAME = "cal_digital"
DEFAULT_CURRENCY = "ILS"
CAL_NUMERIC_CURRENCY_MAP = {
    "3": "ILS",
    "376": "ILS",
}


def _normalize_key(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _find_first(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return None


def _extract_card_id(card: dict[str, Any]) -> str | None:
    direct = _find_first(
        card,
        (
            "cardId",
            "card_id",
            "id",
            "accountId",
            "cardNumber",
            "pan",
            "last4",
        ),
    )
    if direct is not None and str(direct).strip():
        return str(direct)

    for key, value in card.items():
        normalized = _normalize_key(key)
        if "card" in normalized and ("id" in normalized or normalized.endswith("card")):
            if value is not None and str(value).strip():
                return str(value)

    return None


def _extract_card_title(card: dict[str, Any], card_id: str) -> str:
    title = _find_first(
        card,
        (
            "cardName",
            "name",
            "title",
            "description",
            "productName",
            "cardType",
            "brand",
        ),
    )
    title_text = clean_text(str(title)) if title is not None else None
    if title_text:
        return title_text

    last4 = _find_first(card, ("last4", "lastDigits", "maskedNumber"))
    if last4 is not None and str(last4).strip():
        return f"CAL card {last4}"

    return f"CAL card {card_id}"


def _normalize_currency_value(value: Any) -> str | None:
    text = str(value).strip().upper()
    if not text:
        return None
    if text in CAL_NUMERIC_CURRENCY_MAP:
        return CAL_NUMERIC_CURRENCY_MAP[text]
    if len(text) == 3 and text.isalpha():
        return text
    return None


def _extract_currency(source: dict[str, Any] | None = None) -> str:
    if not source:
        return DEFAULT_CURRENCY

    for key in (
        "currency",
        "ccy",
        "trnCurrencyIsoCode",
        "srcCurrencyCode",
        "currencyCode",
        "crmIccCurrencyDesc",
    ):
        value = source.get(key)
        if value in (None, ""):
            continue
        normalized = _normalize_currency_value(value)
        if normalized:
            return normalized

    return DEFAULT_CURRENCY


def _collect_cards(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("cards", "cardList", "items", "data", "results", "content"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        for value in payload.values():
            found = _collect_cards(value)
            if found:
                return found

    return []


def _looks_like_transaction(item: dict[str, Any]) -> bool:
    keys = {_normalize_key(key) for key in item.keys()}
    has_amount = any("amount" in key or "sum" in key or "debit" in key or "credit" in key for key in keys)
    has_date = any("date" in key or "time" in key for key in keys)
    return has_amount and has_date


def _collect_transactions(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        dict_items = [item for item in payload if isinstance(item, dict)]
        if dict_items and any(_looks_like_transaction(item) for item in dict_items):
            return dict_items

        for item in dict_items:
            found = _collect_transactions(item)
            if found:
                return found
        return dict_items

    if isinstance(payload, dict):
        for key in (
            "transactions",
            "operations",
            "items",
            "records",
            "data",
            "results",
            "content",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                dict_items = [item for item in value if isinstance(item, dict)]
                if dict_items:
                    return dict_items

        best: list[dict[str, Any]] = []
        for value in payload.values():
            found = _collect_transactions(value)
            if len(found) > len(best):
                best = found
        return best

    return []


def _extract_datetime(txn: dict[str, Any]) -> datetime:
    raw_value = _find_first(
        txn,
        (
            "trnPurchaseDate",
            "debCrdDate",
            "transactionDate",
            "txnDate",
            "date",
            "eventDate",
            "bookingDate",
            "createdAt",
            "valueDate",
            "timestamp",
        ),
    )
    if raw_value is None:
        return datetime.now(timezone.utc)

    try:
        return parse_datetime(str(raw_value))
    except ValueError:
        return datetime.now(timezone.utc)


def _extract_direction(txn: dict[str, Any]) -> str | None:
    refund_ind = txn.get("refundInd")
    if isinstance(refund_ind, bool):
        return "credit" if refund_ind else "debit"

    value = _find_first(
        txn,
        (
            "direction",
            "debitCredit",
            "type",
            "transactionType",
            "kind",
            "trnType",
            "trnTypeCode",
        ),
    )
    if value is None:
        return None
    return str(value).strip().lower()


def _extract_amount_value(txn: dict[str, Any]) -> Decimal:
    debit_amount = _find_first(txn, ("debitAmount", "debit", "chargeAmount"))
    credit_amount = _find_first(txn, ("creditAmount", "credit", "refundAmount"))
    cal_amount = _find_first(txn, ("trnAmt", "amountForDisplay", "amtBeforeConvAndIndex"))
    generic_amount = _find_first(
        txn,
        (
            "amount",
            "sum",
            "transactionAmount",
            "amountValue",
            "originalAmount",
            "total",
        ),
    )

    for raw in (debit_amount, credit_amount, cal_amount, generic_amount):
        if raw in (None, ""):
            continue
        return quantize_money(abs(parse_decimal(raw)))

    return Decimal("0.00")


def _fallback_direction(merchant: str | None, comment: str | None, txn: dict[str, Any]) -> str:
    context = " ".join(
        [
            merchant or "",
            comment or "",
            str(_find_first(txn, ("type", "transactionType", "kind")) or ""),
            str(_find_first(txn, ("trnType",)) or ""),
        ]
    ).lower()

    positive_markers = ("refund", "credit", "reversal", "chargeback", "זיכוי")
    if any(marker in context for marker in positive_markers):
        return "credit"

    negative_markers = ("debit", "purchase", "payment", "charge", "fee", "חיוב")
    if any(marker in context for marker in negative_markers):
        return "debit"

    return "debit"


def _signed_amount(txn: dict[str, Any], merchant: str | None, comment: str | None) -> Decimal:
    amount = _extract_amount_value(txn)
    if amount == Decimal("0.00"):
        return amount

    direction = _extract_direction(txn)
    if direction is None:
        direction = _fallback_direction(merchant, comment, txn)

    direction_text = str(direction).lower()
    credit_markers = ("credit", "refund", "reversal", "chargeback", "זיכוי", "cr")
    if any(marker in direction_text for marker in credit_markers):
        return abs(amount)
    return -abs(amount)


def _extract_merchant(txn: dict[str, Any]) -> str | None:
    value = _find_first(
        txn,
        (
            "merchant",
            "merchantName",
            "businessName",
            "storeName",
            "description",
            "merchantDescription",
        ),
    )
    return clean_merchant(str(value)) if value is not None else None


def _extract_comment(txn: dict[str, Any]) -> str | None:
    comments = txn.get("comments")
    if isinstance(comments, list):
        parts = [str(item).strip() for item in comments if str(item).strip()]
        if parts:
            return clean_comment(" | ".join(parts))

    value = _find_first(
        txn,
        (
            "comment",
            "memo",
            "note",
            "details",
            "description",
            "remarks",
        ),
    )
    return clean_comment(str(value)) if value is not None else None


def _extract_transaction_id(txn: dict[str, Any]) -> str | None:
    value = _find_first(
        txn,
        (
            "trnIntId",
            "transactionId",
            "txnId",
            "operationId",
            "operation_id",
            "id",
            "referenceId",
            "externalId",
        ),
    )
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _installments_signature(txn: dict[str, Any]) -> str:
    payload = {
        "installments": txn.get("installments"),
        "installmentNumber": txn.get("installmentNumber"),
        "totalInstallments": txn.get("totalInstallments"),
        "paymentNumber": txn.get("paymentNumber"),
        "totalPayments": txn.get("totalPayments"),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _build_external_id(
    *,
    card_id: str,
    txn: dict[str, Any],
    occurred_at: datetime,
    amount: Decimal,
    merchant: str | None,
    comment: str | None,
) -> str:
    transaction_id = _extract_transaction_id(txn)
    if transaction_id:
        return f"cal:{card_id}:{transaction_id}"

    raw_key = "|".join(
        [
            card_id,
            occurred_at.isoformat(),
            f"{amount:.2f}",
            merchant or "",
            comment or "",
            _installments_signature(txn),
        ]
    )
    return hashlib.sha1(raw_key.encode("utf-8")).hexdigest()


def convert_with_diagnostics(raw_bundle: dict[str, Any]) -> tuple[ConnectorResult, dict[str, Any]]:
    cards_payload = raw_bundle.get("cards")
    txns_by_card_raw = raw_bundle.get("txns_by_card", {})

    accounts: list[Account] = []
    movements: list[Movement] = []
    skip_reasons: dict[str, int] = {
        "non_object_txn": 0,
        "duplicate_external_id": 0,
    }
    raw_transactions_by_card: dict[str, int] = {}
    converted_by_card: dict[str, int] = {}

    cards = _collect_cards(cards_payload)
    account_ids_seen: set[str] = set()

    for card in cards:
        card_id = _extract_card_id(card)
        if card_id is None:
            continue

        account_id = f"cal:{card_id}"
        if account_id in account_ids_seen:
            continue

        account_ids_seen.add(account_id)
        accounts.append(
            Account(
                provider=PROVIDER_NAME,
                account_id=account_id,
                title=_extract_card_title(card, card_id),
                currency=_extract_currency(card),
                balance=None,
                meta={
                    "type": "credit_card",
                    "active": True,
                    "source": PROVIDER_NAME,
                    "raw": card,
                },
            )
        )

    seen_movement_ids: set[str] = set()

    for card_id, card_tx_payload in txns_by_card_raw.items():
        if not isinstance(card_id, str):
            card_id = str(card_id)

        account_id = f"cal:{card_id}"
        if account_id not in account_ids_seen:
            accounts.append(
                Account(
                    provider=PROVIDER_NAME,
                    account_id=account_id,
                    title=f"CAL card {card_id}",
                    currency=DEFAULT_CURRENCY,
                    balance=None,
                    meta={
                        "type": "credit_card",
                        "active": True,
                        "source": PROVIDER_NAME,
                    },
                )
            )
            account_ids_seen.add(account_id)

        transactions = _collect_transactions(card_tx_payload)
        card_key = card_id or "unknown"
        raw_transactions_by_card[card_key] = len(transactions)
        converted_before = len(movements)
        for txn in transactions:
            if not isinstance(txn, dict):
                skip_reasons["non_object_txn"] += 1
                continue

            merchant = _extract_merchant(txn)
            comment = _extract_comment(txn)
            occurred_at = _extract_datetime(txn)
            amount = _signed_amount(txn, merchant, comment)
            currency = _extract_currency(txn)
            external_id = _build_external_id(
                card_id=card_id,
                txn=txn,
                occurred_at=occurred_at,
                amount=amount,
                merchant=merchant,
                comment=comment,
            )

            if external_id in seen_movement_ids:
                skip_reasons["duplicate_external_id"] += 1
                continue
            seen_movement_ids.add(external_id)

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
                    transfer_id=None,
                    meta={
                        "source": PROVIDER_NAME,
                        "raw": txn,
                    },
                )
            )
        converted_by_card[card_key] = len(movements) - converted_before

    total_raw_transactions = sum(raw_transactions_by_card.values())
    converted_movements = len(movements)
    skipped_in_converter = max(0, total_raw_transactions - converted_movements)
    diagnostics = {
        "raw_transactions_total": total_raw_transactions,
        "raw_transactions_by_card": raw_transactions_by_card,
        "converted_movements": converted_movements,
        "converted_by_card": converted_by_card,
        "skipped_in_converter": skipped_in_converter,
        "skip_reasons": {key: value for key, value in skip_reasons.items() if value > 0},
    }
    return ConnectorResult(accounts=accounts, movements=movements), diagnostics


def convert(raw_bundle: dict[str, Any]) -> ConnectorResult:
    result, _diagnostics = convert_with_diagnostics(raw_bundle)
    return result
