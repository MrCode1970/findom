from __future__ import annotations

import os
from typing import TypeVar

T = TypeVar("T")

def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def column_letter_to_index(letter: str) -> int:
    text = letter.strip().upper()
    if not text or any(not ("A" <= ch <= "Z") for ch in text):
        raise ValueError(f"Invalid column letter: {letter}")

    value = 0
    for ch in text:
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value - 1


def index_to_column_letter(index: int) -> str:
    if index < 0:
        raise ValueError("Column index must be >= 0")
    value = index + 1
    chars: list[str] = []
    while value > 0:
        value, rem = divmod(value - 1, 26)
        chars.append(chr(rem + ord("A")))
    return "".join(reversed(chars))


def parse_columns_span(span: str) -> tuple[str, str]:
    text = span.strip().upper()
    if ":" not in text:
        raise ValueError(f"Invalid columns span: {span}")
    left, right = [part.strip() for part in text.split(":", 1)]
    column_letter_to_index(left)
    column_letter_to_index(right)
    return left, right


def chunk_list(items: list[T], size: int) -> list[list[T]]:
    if size <= 0:
        raise ValueError("Chunk size must be positive")
    return [items[i : i + size] for i in range(0, len(items), size)]
