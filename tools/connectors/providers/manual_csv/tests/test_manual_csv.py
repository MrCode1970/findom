from datetime import date
from decimal import Decimal

from tools.connectors._core.types import SyncWindow
from tools.connectors.providers.manual_csv import api, converters


def _load_result():
    raw = api.sync(
        SyncWindow(from_date=date(2026, 2, 1), to_date=date(2026, 2, 28)),
        state={},
    )
    return converters.convert(raw)


def test_csv_row_conversion_decimal_and_iso_date() -> None:
    result = _load_result()
    coffee = next(m for m in result.movements if m.merchant == "Coffee Shop")

    assert coffee.amount == Decimal("-10.50")
    assert coffee.occurred_at.isoformat() == "2026-02-20T00:00:00+00:00"


def test_external_id_is_stable_between_runs() -> None:
    first = _load_result()
    second = _load_result()

    first_ids = [movement.external_id for movement in first.movements]
    second_ids = [movement.external_id for movement in second.movements]

    assert first_ids == second_ids


def test_transfer_group_maps_to_same_transfer_id() -> None:
    result = _load_result()
    transfer_movements = [m for m in result.movements if m.transfer_id is not None]

    assert len(transfer_movements) == 2
    assert transfer_movements[0].transfer_id == transfer_movements[1].transfer_id
    assert transfer_movements[0].transfer_id == "tr-001"
