from datetime import date
from decimal import Decimal

from tools.connectors._core.types import SyncWindow
from tools.connectors.providers._template import api, converters


def test_template_converter_outputs_movement() -> None:
    raw = api.sync(
        SyncWindow(from_date=date(2026, 2, 1), to_date=date(2026, 2, 28)),
        state={},
    )

    result = converters.convert(raw)

    assert len(result.accounts) == 1
    assert len(result.movements) == 1
    movement = result.movements[0]

    assert movement.amount == Decimal("-10.50")
    assert movement.occurred_at.isoformat() == "2026-02-20T00:00:00+00:00"
