from __future__ import annotations

from datetime import date, datetime, time, timezone, tzinfo


_DATE_FORMATS = ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d")
_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
)


def parse_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = value.strip()
    if "T" in text:
        return parse_datetime(text).date()

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {value}")


def parse_datetime(value: str | date | datetime, tz: tzinfo = timezone.utc) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = None

        if parsed is None:
            for fmt in _DATETIME_FORMATS + _DATE_FORMATS:
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue

        if parsed is None:
            raise ValueError(f"Unsupported datetime format: {value}")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)

    return parsed.astimezone(tz)


def to_iso_datetime(value: str | date | datetime, tz: tzinfo = timezone.utc) -> str:
    return parse_datetime(value, tz=tz).isoformat()
