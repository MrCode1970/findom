from __future__ import annotations

import pytest

from gsheets_toolkit.values import normalize_rows
from scripts.sources.registry import _normalize_required_amount


@pytest.mark.parametrize(
    ("raw_amount", "expected"),
    [
        (15.8, 15.8),
        ("15.8", 15.8),
        ("'15.8", 15.8),
        (-292.06, -292.06),
        ("-292.06", -292.06),
    ],
)
def test_normalize_required_amount_accepts_numeric_inputs(raw_amount: object, expected: float) -> None:
    movement = {"amount": raw_amount}
    actual = _normalize_required_amount(movement, source="cal_digital", index=1)
    assert actual == pytest.approx(expected)


def test_normalize_required_amount_supports_comma_decimal() -> None:
    movement = {"amount": "'15,8"}
    actual = _normalize_required_amount(movement, source="cal_digital", index=1)
    assert actual == pytest.approx(15.8)


def test_normalize_required_amount_rejects_non_numeric() -> None:
    movement = {"amount": "abc"}
    with pytest.raises(RuntimeError, match="non-numeric amount"):
        _normalize_required_amount(movement, source="cal_digital", index=1)


def test_normalize_rows_preserves_numeric_types() -> None:
    rows = normalize_rows([["2026-03-01", 15.8, -292.06]], "A:C")
    assert isinstance(rows[0][1], float)
    assert isinstance(rows[0][2], float)
