from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    key: str
    header: str
    width: int | None = None
    hidden: bool = False


@dataclass(frozen=True, slots=True)
class SheetSchema:
    title: str
    columns: list[ColumnSpec] = field(default_factory=list)
    freeze_rows: int = 1
    filter: bool = True


@dataclass(frozen=True, slots=True)
class SpreadsheetSchema:
    sheets: list[SheetSchema] = field(default_factory=list)
    version: int = 1

