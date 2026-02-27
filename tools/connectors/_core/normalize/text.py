from __future__ import annotations


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = " ".join(str(value).split())
    return normalized or None


def clean_merchant(value: str | None) -> str | None:
    return clean_text(value)


def clean_comment(value: str | None) -> str | None:
    return clean_text(value)
